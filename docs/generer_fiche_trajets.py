"""Génère docs/FICHE_SOURCE_TRAJETS.md à partir des fichiers Parquet réels.

Tous les chiffres de la fiche sont mesurés sur les fichiers avec DuckDB :
taille, lignes, colonnes, types, exemples, répartition des codes, anomalies.
Les significations et les codes viennent du dictionnaire de données TLC
(version du 18 mars 2025).

Usage, depuis la racine du projet :
    pip install -r docs/requirements.txt
    python docs/generer_fiche_trajets.py /tmp/tlc/yellow_tripdata_2025-01.parquet

Plusieurs fichiers possibles : le volume est mesuré pour chacun, l'analyse
détaillée (colonnes, codes, anomalies) porte sur le premier, et les écarts de
schéma entre fichiers sont signalés.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

SORTIE_DEFAUT = Path(__file__).resolve().parent / "FICHE_SOURCE_TRAJETS.md"
URL_MODELE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_AAAA-MM.parquet"
URL_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
URL_DICTIONNAIRE = "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf"

# Signification de chaque colonne (dictionnaire TLC), clé en minuscules
SIGNIFICATIONS = {
    "vendorid": "Fournisseur du système d'enregistrement (TPEP) qui a transmis le trajet",
    "tpep_pickup_datetime": "Date et heure de prise en charge (compteur enclenché), heure locale de New York",
    "tpep_dropoff_datetime": "Date et heure de dépose (compteur arrêté), heure locale de New York",
    "passenger_count": "Nombre de passagers, saisi par le chauffeur",
    "trip_distance": "Distance parcourue en miles, mesurée par le taximètre",
    "ratecodeid": "Tarif appliqué en fin de trajet",
    "store_and_fwd_flag": "Trajet stocké dans le véhicule faute de connexion, puis transmis",
    "pulocationid": "Zone TLC de prise en charge (voir taxi_zone_lookup.csv)",
    "dolocationid": "Zone TLC de dépose (voir taxi_zone_lookup.csv)",
    "payment_type": "Mode de paiement",
    "fare_amount": "Prix de la course calculé par le taximètre (temps et distance), en dollars",
    "extra": "Suppléments divers (heures de pointe, nuit), en dollars",
    "mta_tax": "Taxe MTA déclenchée automatiquement selon le tarif, en dollars",
    "tip_amount": "Pourboire, rempli automatiquement pour les cartes (espèces non incluses), en dollars",
    "tolls_amount": "Total des péages payés pendant le trajet, en dollars",
    "improvement_surcharge": "Supplément d'amélioration perçu à la prise en charge, en dollars",
    "total_amount": "Montant total facturé au passager, hors pourboires en espèces, en dollars",
    "congestion_surcharge": "Supplément de congestion de l'État de New York, en dollars",
    "airport_fee": "Frais de prise en charge aux aéroports LaGuardia et JFK, en dollars",
    "cbd_congestion_fee": "Redevance de la zone de péage urbain de Manhattan (MTA), depuis le 5 janvier 2025, en dollars",
}

# Codes documentés par le dictionnaire TLC
CODES = {
    "vendorid": {
        1: "Creative Mobile Technologies, LLC",
        2: "Curb Mobility, LLC",
        6: "Myle Technologies Inc",
        7: "Helix",
    },
    "ratecodeid": {
        1: "Tarif standard",
        2: "JFK",
        3: "Newark",
        4: "Nassau ou Westchester",
        5: "Tarif négocié",
        6: "Trajet partagé",
        99: "Non renseigné",
    },
    "store_and_fwd_flag": {
        "Y": "Stocké dans le véhicule puis transmis plus tard",
        "N": "Transmis directement",
    },
    "payment_type": {
        0: "Flex Fare (tarif flexible)",
        1: "Carte bancaire",
        2: "Espèces",
        3: "Gratuit",
        4: "Litige",
        5: "Inconnu",
        6: "Trajet annulé",
    },
}

MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet",
           "août", "septembre", "octobre", "novembre", "décembre"]


# --------------------------------------------------------------------------
# Mise en forme
# --------------------------------------------------------------------------
def entier(n: int | None) -> str:
    """3475226 → '3 475 226'."""
    return "–" if n is None else f"{n:,}".replace(",", " ")


def pourcent(part: int, total: int) -> str:
    if not total or not part:
        return "0 %"
    taux = part / total * 100
    return "< 0,01 %" if taux < 0.01 else f"{taux:.2f} %".replace(".", ",")


def accord(n: int, singulier: str, pluriel: str) -> str:
    return singulier if n <= 1 else pluriel


def valeur(v) -> str:
    if v is None:
        return "(vide)"
    if isinstance(v, (float, Decimal)):
        return f"{float(v):g}".replace(".", ",")
    return str(v).replace("|", "\\|")


def ident(nom: str) -> str:
    """Nom de colonne entre guillemets doubles pour DuckDB."""
    return '"' + nom.replace('"', '""') + '"'


def chaine_sql(texte: str) -> str:
    return "'" + texte.replace("'", "''") + "'"


# --------------------------------------------------------------------------
# Mesures
# --------------------------------------------------------------------------
def mesurer_fichier(con: duckdb.DuckDBPyConnection, fichier: Path) -> dict:
    source = f"read_parquet({chaine_sql(str(fichier))})"
    colonnes = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    meta = con.execute(
        f"SELECT string_agg(DISTINCT compression, ', '), COUNT(DISTINCT row_group_id) "
        f"FROM parquet_metadata({chaine_sql(str(fichier))})"
    ).fetchone()
    return {
        "fichier": fichier,
        "source": source,
        "taille": fichier.stat().st_size,
        "lignes": con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0],
        "colonnes": [(c[0], c[1]) for c in colonnes],
        "compression": meta[0],
        "row_groups": meta[1],
    }


def mois_du_fichier(nom: str) -> date | None:
    trouve = re.search(r"(\d{4})-(\d{2})", nom)
    return date(int(trouve[1]), int(trouve[2]), 1) if trouve else None


def analyser(con: duckdb.DuckDBPyConnection, mesure: dict) -> dict:
    source, total = mesure["source"], mesure["lignes"]
    noms = [c for c, _ in mesure["colonnes"]]
    col = {c.lower(): c for c in noms}  # minuscules → nom réel dans le fichier

    # Exemple (valeur la plus fréquente) et nombre de valeurs vides par colonne
    selection = ", ".join(
        f"mode({ident(c)}), COUNT(*) - COUNT({ident(c)})" for c in noms
    )
    ligne = con.execute(f"SELECT {selection} FROM {source}").fetchone()
    exemples = {c: ligne[2 * i] for i, c in enumerate(noms)}
    vides = {c: ligne[2 * i + 1] for i, c in enumerate(noms)}

    # Répartition des codes
    repartition = {}
    for cle in CODES:
        if cle in col:
            repartition[cle] = con.execute(
                f"SELECT {ident(col[cle])}, COUNT(*) FROM {source} GROUP BY 1 ORDER BY 1 NULLS LAST"
            ).fetchall()

    # Anomalies
    anomalies = {}
    debut = mois_du_fichier(mesure["fichier"].name)
    pickup, dropoff = col.get("tpep_pickup_datetime"), col.get("tpep_dropoff_datetime")
    if pickup:
        anomalies["pickup_min"], anomalies["pickup_max"] = con.execute(
            f"SELECT MIN({ident(pickup)}), MAX({ident(pickup)}) FROM {source}"
        ).fetchone()
    if pickup and debut:
        anomalies["hors_mois"] = con.execute(
            f"SELECT COUNT(*) FROM {source} WHERE {ident(pickup)} < DATE {chaine_sql(str(debut))} "
            f"OR {ident(pickup)} >= DATE {chaine_sql(str(debut))} + INTERVAL 1 MONTH"
        ).fetchone()[0]
    if pickup and dropoff:
        anomalies["duree_negative"], anomalies["duree_longue"] = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {ident(dropoff)} <= {ident(pickup)}), "
            f"COUNT(*) FILTER (WHERE date_diff('minute', {ident(pickup)}, {ident(dropoff)}) > 180) "
            f"FROM {source}"
        ).fetchone()
    if "trip_distance" in col:
        anomalies["distance_nulle"], anomalies["distance_longue"] = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {ident(col['trip_distance'])} = 0), "
            f"COUNT(*) FILTER (WHERE {ident(col['trip_distance'])} > 100) FROM {source}"
        ).fetchone()
    if "total_amount" in col:
        anomalies["montant_negatif"], anomalies["montant_min"] = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {ident(col['total_amount'])} < 0), "
            f"MIN({ident(col['total_amount'])}) FROM {source}"
        ).fetchone()
    if "passenger_count" in col:
        anomalies["zero_passager"] = con.execute(
            f"SELECT COUNT(*) FROM {source} WHERE {ident(col['passenger_count'])} = 0"
        ).fetchone()[0]

    return {"col": col, "exemples": exemples, "vides": vides,
            "repartition": repartition, "anomalies": anomalies, "debut": debut}


# --------------------------------------------------------------------------
# Rédaction de la fiche
# --------------------------------------------------------------------------
def surprises(mesure: dict, analyse: dict, autres: list[dict]) -> list[str]:
    total, a, col = mesure["lignes"], analyse["anomalies"], analyse["col"]
    debut = analyse["debut"]
    lignes = []

    if "hors_mois" in a and debut:
        lignes.append(
            f"**Dates hors période** : {entier(a['hors_mois'])} trajets ({pourcent(a['hors_mois'], total)}) "
            f"ont une prise en charge en dehors de {MOIS_FR[debut.month - 1]} {debut.year} ; "
            f"les dates du fichier vont du {a['pickup_min']:%d/%m/%Y} au {a['pickup_max']:%d/%m/%Y}."
        )
    if "distance_nulle" in a:
        lignes.append(
            f"**Distances** : {entier(a['distance_nulle'])} trajets ({pourcent(a['distance_nulle'], total)}) "
            f"ont une distance nulle, et {entier(a['distance_longue'])} "
            f"{accord(a['distance_longue'], 'dépasse', 'dépassent')} 100 miles."
        )
    if "montant_negatif" in a:
        lignes.append(
            f"**Montants négatifs** : {entier(a['montant_negatif'])} trajets "
            f"({pourcent(a['montant_negatif'], total)}) ont un `total_amount` négatif "
            f"(minimum : {valeur(a['montant_min'])} $), probablement des annulations ou des corrections."
        )
    if "duree_negative" in a:
        lignes.append(
            f"**Durées** : {entier(a['duree_negative'])} trajets se terminent avant ou au moment de "
            f"leur début, et {entier(a['duree_longue'])} "
            f"{accord(a['duree_longue'], 'dure', 'durent')} plus de 3 heures."
        )
    if "zero_passager" in a:
        lignes.append(
            f"**Passagers** : {entier(a['zero_passager'])} trajets déclarent 0 passager."
        )

    vides = {c: n for c, n in analyse["vides"].items() if n}
    if vides:
        detail = ", ".join(f"`{c}` ({entier(n)})" for c, n in sorted(vides.items(), key=lambda x: -x[1]))
        lignes.append(f"**Valeurs vides** : {detail}.")

    hors_dico = []
    for cle, lignes_codes in analyse["repartition"].items():
        connus = CODES[cle]
        inconnus = [v for v, _ in lignes_codes if v is not None and v not in connus]
        if inconnus:
            hors_dico.append(f"`{col[cle]}` : {', '.join(valeur(v) for v in inconnus)}")
    if hors_dico:
        lignes.append(f"**Codes absents du dictionnaire** : {' ; '.join(hors_dico)}.")

    majuscules = [c for c, _ in mesure["colonnes"] if c != c.lower()]
    if majuscules:
        lignes.append(
            f"**Noms de colonnes** : la casse varie ({', '.join(f'`{c}`' for c in majuscules)}). "
            "D'où `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` au chargement."
        )

    entiers_en_reel = [
        c for c, t in mesure["colonnes"]
        if c.lower() in ("passenger_count", "ratecodeid", "payment_type", "vendorid") and t.upper() == "DOUBLE"
    ]
    if entiers_en_reel:
        lignes.append(
            f"**Types** : des codes entiers sont stockés en nombre réel "
            f"({', '.join(f'`{c}`' for c in entiers_en_reel)})."
        )

    # Comparaison sans tenir compte de la casse, comme le chargement
    schema = {c.lower(): (c, t) for c, t in mesure["colonnes"]}
    for autre in autres:
        sch = {c.lower(): (c, t) for c, t in autre["colonnes"]}
        en_plus = sorted(sch[k][0] for k in set(sch) - set(schema))
        en_moins = sorted(schema[k][0] for k in set(schema) - set(sch))
        communes = sorted(set(sch) & set(schema))
        casse = [k for k in communes if sch[k][0] != schema[k][0]]
        types = [k for k in communes if sch[k][1] != schema[k][1]]
        ecarts = []
        if en_plus:
            ecarts.append("colonnes en plus : " + ", ".join(f"`{c}`" for c in en_plus))
        if en_moins:
            ecarts.append("colonnes absentes : " + ", ".join(f"`{c}`" for c in en_moins))
        if casse:
            ecarts.append("casse différente : " + ", ".join(
                f"`{schema[k][0]}` → `{sch[k][0]}`" for k in casse))
        if types:
            ecarts.append("types différents : " + ", ".join(
                f"`{sch[k][0]}` ({schema[k][1]} → {sch[k][1]})" for k in types))
        if ecarts:
            lignes.append(f"**Schéma de {autre['fichier'].name}** : {' ; '.join(ecarts)}.")
    return lignes


def rediger(mesures: list[dict], analyse: dict) -> str:
    principale, autres = mesures[0], mesures[1:]
    commande = "python docs/generer_fiche_trajets.py " + " ".join(m["fichier"].name for m in mesures)
    out = []
    w = out.append

    w("# Fiche source — Trajets des taxis jaunes de New York (TLC)\n")
    w(f"> Fiche générée le {date.today():%d/%m/%Y} par `docs/generer_fiche_trajets.py` : "
      f"tous les chiffres sont mesurés sur les fichiers avec DuckDB {duckdb.__version__}. "
      f"Analyse détaillée : `{principale['fichier'].name}`.\n")

    w("## Identité\n")
    w("| Rubrique | Réponse |")
    w("|---|---|")
    w("| Nom de la source | Yellow Taxi Trip Records : un enregistrement par trajet de taxi jaune |")
    w("| Producteur des données | NYC Taxi and Limousine Commission (TLC), à partir des données transmises "
      "par les fournisseurs de systèmes d'enregistrement (TPEP) |")
    w(f"| Adresse (URL) | `{URL_MODELE}` ; page : {URL_PAGE} ; dictionnaire : {URL_DICTIONNAIRE} |")
    w("| Accès (public, authentifié) | Public, sans authentification |")
    w(f"| Format du fichier | Apache Parquet (compression {principale['compression']}, "
      f"{principale['row_groups']} groupes de lignes), un fichier par mois |")
    w("| Fréquence de publication | Mensuelle |")
    w("| Délai entre la période couverte et la publication | Environ deux mois (« typically with a "
      "two-month delay to allow time for full vendor submissions », page TLC) |")
    w("")

    w("## Volume mesuré\n")
    w("| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |")
    w("|---|---|---|---|---|")
    for m in mesures:
        mo = f"{m['taille'] / 1e6:.1f}".replace(".", ",")
        w(f"| `{m['fichier'].name}` | {mo} Mo ({entier(m['taille'])} octets) "
          f"| {entier(m['lignes'])} | {len(m['colonnes'])} | DuckDB : `{commande}` |")
    w("")

    w("## Colonnes\n")
    w("| Colonne | Type dans le fichier | Signification | Exemple de valeur |")
    w("|---|---|---|---|")
    for nom, type_ in principale["colonnes"]:
        signification = SIGNIFICATIONS.get(nom.lower(), "(absente du dictionnaire)")
        w(f"| `{nom}` | {type_} | {signification} | {valeur(analyse['exemples'][nom])} |")
    w("\nExemple de valeur : la valeur la plus fréquente de la colonne.\n")

    w("## Codes\n")
    w(f"D'après le dictionnaire de données TLC (version du 18 mars 2025), avec la répartition "
      f"mesurée dans `{principale['fichier'].name}`.\n")
    w("| Colonne | Valeur | Signification | Trajets |")
    w("|---|---|---|---|")
    total = principale["lignes"]
    for cle, lignes_codes in analyse["repartition"].items():
        nom = analyse["col"][cle]
        presents = {v for v, _ in lignes_codes}
        for v, n in lignes_codes:
            sens = "Non renseigné" if v is None else CODES[cle].get(v, "**Hors dictionnaire**")
            w(f"| `{nom}` | {valeur(v)} | {sens} | {entier(n)} ({pourcent(n, total)}) |")
        for v, sens in CODES[cle].items():
            if v not in presents:
                w(f"| `{nom}` | {valeur(v)} | {sens} | 0 |")
    w("")

    w("## Ce qui a surpris\n")
    for ligne in surprises(principale, analyse, autres):
        w(f"- {ligne}")
    w("")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Génère la fiche source des trajets TLC.")
    parser.add_argument("fichiers", nargs="+", type=Path, help="fichiers Parquet mensuels")
    parser.add_argument("-o", "--sortie", type=Path, default=SORTIE_DEFAUT,
                        help=f"fiche à écrire (défaut : {SORTIE_DEFAUT.name})")
    args = parser.parse_args()

    absents = [str(f) for f in args.fichiers if not f.is_file()]
    if absents:
        parser.error(f"fichier(s) introuvable(s) : {', '.join(absents)}")

    con = duckdb.connect()
    mesures = [mesurer_fichier(con, f) for f in args.fichiers]
    analyse = analyser(con, mesures[0])
    args.sortie.write_text(rediger(mesures, analyse), encoding="utf-8")
    print(f"Fiche écrite : {args.sortie}")
    for m in mesures:
        print(f"  {m['fichier'].name} : {entier(m['lignes'])} lignes, "
              f"{len(m['colonnes'])} colonnes, {m['taille'] / 1e6:.1f} Mo")
    return 0


if __name__ == "__main__":
    sys.exit(main())
