"""Charge un mois de trajets TLC, ou la liste des zones, dans NYC_TAXI.RAW.

Usage :
    python ingestion/charger_mois.py 2025-01     # trajets de janvier 2025
    python ingestion/charger_mois.py --zones     # liste des 265 zones

Étapes : téléchargement du fichier public → PUT sur le stage → COPY INTO.

Rejouable : relancer un mois déjà chargé n'ajoute aucune ligne.
  - PUT ... OVERWRITE=FALSE n'envoie pas de nouveau un fichier déjà présent
    sur le stage (statut SKIPPED) ;
  - COPY INTO ignore un fichier déjà chargé dans la table : Snowflake garde
    cette mémoire 64 jours (« Copy executed with 0 files processed »).

Configuration par variables d'environnement, aucun secret dans le code.
Le fichier .env à la racine du projet (ignoré par Git) sert de valeur de
repli : une variable déjà définie dans le terminal reste prioritaire.
    SNOWFLAKE_ACCOUNT            identifiant ORGANISATION-COMPTE (obligatoire)
    SNOWFLAKE_USER               défaut : AIRFLOW_SVC
    SNOWFLAKE_ROLE               défaut : TRANSFORMER
    SNOWFLAKE_WAREHOUSE          défaut : NYC_TAXI_WH
    SNOWFLAKE_PRIVATE_KEY_PATH   défaut : ~/.ssh/snowflake/rsa_key.p8
    DOSSIER_TELECHARGEMENT       défaut : /tmp/tlc (hors du dépôt Git)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

RACINE_PROJET = Path(__file__).resolve().parent.parent

URL_TRAJETS = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{mois}.parquet"
URL_ZONES = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

STAGE = "NYC_TAXI.RAW.TLC_STAGE"
TABLE_TRAJETS = "NYC_TAXI.RAW.YELLOW_TRIPDATA"
TABLE_ZONES = "NYC_TAXI.RAW.TAXI_ZONE_LOOKUP"
FORMAT_PARQUET = "NYC_TAXI.RAW.PARQUET_FF"
FORMAT_CSV = "NYC_TAXI.RAW.CSV_FF"

MOIS_VALIDE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def connexion() -> snowflake.connector.SnowflakeConnection:
    """Ouvre une session Snowflake avec l'utilisateur de service et sa clé privée."""
    chemin_cle = Path(
        os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "~/.ssh/snowflake/rsa_key.p8")
    ).expanduser()
    cle = serialization.load_pem_private_key(chemin_cle.read_bytes(), password=None)
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ.get("SNOWFLAKE_USER", "AIRFLOW_SVC"),
        private_key=cle,
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NYC_TAXI_WH"),
        database="NYC_TAXI",
        schema="RAW",
    )


def telecharger(url: str, dossier: Path) -> Path:
    """Télécharge le fichier par morceaux, sans le garder en mémoire.

    Une copie locale complète est réutilisée. Le téléchargement passe par un
    fichier .part : une coupure ne laisse jamais un fichier tronqué sous le
    nom définitif.
    """
    dossier.mkdir(parents=True, exist_ok=True)
    destination = dossier / url.rsplit("/", 1)[-1]
    if destination.exists():
        print(f"Déjà téléchargé : {destination}")
        return destination

    temporaire = destination.with_name(destination.name + ".part")
    print(f"Téléchargement : {url}")
    with requests.get(url, stream=True, timeout=120) as reponse:
        # 403 ou 404 : le fichier de ce mois n'est pas (encore) publié
        reponse.raise_for_status()
        with temporaire.open("wb") as f:
            for morceau in reponse.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(morceau)
    temporaire.rename(destination)
    print(f"Téléchargé : {destination} ({destination.stat().st_size / 1e6:.1f} Mo)")
    return destination


def deposer(cur, fichier: Path) -> str:
    """Envoie le fichier à la racine du stage, sans le compresser ni l'écraser."""
    cur.execute(
        f"PUT 'file://{fichier.resolve()}' @{STAGE} AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
    )
    statut = cur.fetchone()[6]  # UPLOADED, ou SKIPPED si déjà sur le stage
    print(f"PUT {fichier.name} : {statut}")
    return statut


def copier(cur, nom_fichier: str, table: str, format_fichier: str) -> int:
    """Copie le fichier du stage dans la table. Renvoie le nombre de lignes ajoutées."""
    cur.execute(
        f"""
        COPY INTO {table}
        FROM @{STAGE}
        FILES = ('{nom_fichier}')
        FILE_FORMAT = (FORMAT_NAME = {format_fichier})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        INCLUDE_METADATA = (_source_file = METADATA$FILENAME, _loaded_at = METADATA$START_SCAN_TIME)
        ON_ERROR = ABORT_STATEMENT
        """
    )
    colonnes = [c[0].lower() for c in cur.description]
    ajoutees = 0
    for ligne in cur.fetchall():
        resultat = dict(zip(colonnes, ligne))
        if "rows_loaded" in resultat:
            print(
                f"COPY {resultat['file']} : {resultat['status']}, "
                f"{resultat['rows_loaded']} lignes chargées"
            )
            ajoutees += int(resultat["rows_loaded"] or 0)
        else:
            # Fichier déjà chargé : « Copy executed with 0 files processed. »
            print(f"COPY : {resultat.get('status', ligne)}")
    return ajoutees


def compter(cur, table: str, nom_fichier: str) -> int:
    """Nombre de lignes de la table qui viennent de ce fichier."""
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE _source_file = %s", (nom_fichier,))
    return cur.fetchone()[0]


def main() -> int:
    # override=False : les variables du terminal gardent la priorité sur le .env
    load_dotenv(RACINE_PROJET / ".env", override=False)

    parser = argparse.ArgumentParser(
        description="Charge un mois de trajets TLC ou la liste des zones dans NYC_TAXI.RAW."
    )
    parser.add_argument("mois", nargs="?", help="mois à charger, au format AAAA-MM (ex. 2025-01)")
    parser.add_argument("--zones", action="store_true", help="charger la liste des zones TLC")
    args = parser.parse_args()

    if bool(args.mois) == args.zones:
        parser.error("indiquer soit un mois (AAAA-MM), soit --zones")
    if args.mois and not MOIS_VALIDE.match(args.mois):
        parser.error(f"mois invalide : {args.mois} (format attendu : AAAA-MM)")
    if not os.environ.get("SNOWFLAKE_ACCOUNT"):
        parser.error("variable d'environnement SNOWFLAKE_ACCOUNT manquante (format ORGANISATION-COMPTE)")

    if args.zones:
        url, table, format_fichier = URL_ZONES, TABLE_ZONES, FORMAT_CSV
    else:
        url, table, format_fichier = URL_TRAJETS.format(mois=args.mois), TABLE_TRAJETS, FORMAT_PARQUET

    fichier = telecharger(url, Path(os.environ.get("DOSSIER_TELECHARGEMENT", "/tmp/tlc")))

    with connexion() as conn, conn.cursor() as cur:
        deposer(cur, fichier)
        ajoutees = copier(cur, fichier.name, table, format_fichier)
        total = compter(cur, table, fichier.name)

    print(f"Bilan : {ajoutees} lignes ajoutées ; {total} lignes de {fichier.name} dans {table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
