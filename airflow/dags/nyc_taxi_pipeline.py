"""Pipeline mensuel NYC Yellow Taxi : chargement de la couche RAW.

Une exécution traite un mois : celui de sa date logique (logical_date), jamais
celui de la date du jour. Avec catchup=True, Airflow crée une exécution par mois
entre PREMIER_MOIS et DERNIER_MOIS.

    vérifier que le fichier du mois est publié
      → le télécharger et le déposer sur le stage (PUT)
      → le copier dans la table RAW (COPY INTO)

Configuration : les noms (connexion, source, stage, table) sont des params du
DAG, rendus par Jinja ({{ params.xxx }}) au moment de l'exécution. Pour réutiliser
ce DAG sur un autre projet, seul le bloc CONFIGURATION ci-dessous change.

Rejouable : relancer un mois n'ajoute aucune ligne. PUT ... OVERWRITE=FALSE
n'envoie pas de nouveau un fichier déjà présent, et COPY INTO ignore un fichier
déjà chargé dans la table (mémoire de 64 jours côté Snowflake).

Connexion : son contenu (compte, clé privée, rôle) est dans airflow/.env, ignoré
par Git et par Docker : aucune clé dans le code ni dans l'image.

Ne pas utiliser le bouton Trigger : une exécution manuelle prend la date du jour,
et le fichier de ce mois n'existe pas.
"""

from pathlib import Path

import pendulum
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Param, dag, task

# =============================================================================
# CONFIGURATION DU PROJET : seul bloc à modifier pour un autre projet
# =============================================================================

# Période rejouée : une exécution par mois, du premier au dernier inclus
PREMIER_MOIS = pendulum.datetime(2025, 1, 1, tz="UTC")
DERNIER_MOIS = pendulum.datetime(2025, 3, 1, tz="UTC")

# Valeurs accessibles en Jinja par {{ params.<nom> }}, dans ce DAG comme dans
# les fichiers SQL. Visibles dans l'interface, onglet Détails du DAG.
PARAMS = {
    # Connexion Airflow vers Snowflake
    "conn_id": Param(
        "snowflake_nyc_taxi", type="string",
        description="Identifiant de la connexion Airflow (définie dans airflow/.env)",
    ),
    # Source des fichiers mensuels : <url_base>/<prefixe_fichier>_AAAA-MM.<extension>
    "url_base": Param(
        "https://d37ci6vzurychx.cloudfront.net/trip-data", type="string",
        description="Adresse du dossier des fichiers mensuels, sans / final",
    ),
    "prefixe_fichier": Param(
        "yellow_tripdata", type="string",
        description="Début du nom des fichiers, avant _AAAA-MM",
    ),
    "extension": Param("parquet", type="string", description="Extension des fichiers"),
    # Destination dans Snowflake (objets créés par snowflake/02_raw.sql)
    "stage": Param(
        "NYC_TAXI.RAW.TLC_STAGE", type="string",
        description="Stage interne où sont déposés les fichiers",
    ),
    "format_fichier": Param(
        "NYC_TAXI.RAW.PARQUET_FF", type="string",
        description="Format de fichier utilisé par COPY INTO",
    ),
    "table_brute": Param(
        "NYC_TAXI.RAW.YELLOW_TRIPDATA", type="string",
        description="Table RAW alimentée chaque mois",
    ),
    # Paramètres attendus par les fichiers SQL fournis (transformations, jour 4)
    "max_trip_distance_miles": Param(
        100, type="integer",
        description="Distance au-delà de laquelle un trajet est écarté (int_trips__flagged.sql)",
    ),
    "max_trip_duration_min": Param(
        180, type="integer",
        description="Durée au-delà de laquelle un trajet est écarté (int_trips__flagged.sql)",
    ),
    "start_month": Param(
        PREMIER_MOIS.to_date_string(), type="string",
        description="Premier mois du calendrier (dim_date.sql)",
    ),
    "end_month": Param(
        DERNIER_MOIS.add(months=1).to_date_string(), type="string",
        description="Mois qui suit le dernier mois du calendrier, exclu (dim_date.sql)",
    ),
}

# =============================================================================

# Gabarits Jinja, rendus par Airflow au moment de chaque exécution
MOIS = "{{ logical_date.strftime('%Y-%m') }}"  # ex. 2025-01, comme dans les SQL fournis
FICHIER = "{{ params.prefixe_fichier }}_" + MOIS + ".{{ params.extension }}"
URL = "{{ params.url_base }}/" + FICHIER


@dag(
    dag_id="nyc_taxi_pipeline",
    description="Chargement mensuel des trajets TLC dans la couche RAW",
    schedule="@monthly",
    start_date=PREMIER_MOIS,
    end_date=DERNIER_MOIS,
    catchup=True,  # False par défaut en Airflow 3 : indispensable pour rejouer l'historique
    max_active_runs=1,  # un mois à la fois, dans l'ordre
    params=PARAMS,
    default_args={
        # Coupure réseau, fichier pas encore publié, Snowflake indisponible
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["nyc_taxi", "raw"],
)
def nyc_taxi_pipeline():
    @task
    def verifier_disponibilite(url: str) -> str:
        """Échoue si le fichier du mois n'est pas publié (403 ou 404 chez la source)."""
        requests.head(url, timeout=30, allow_redirects=True).raise_for_status()
        print(f"Fichier disponible : {url}")
        return url

    @task
    def telecharger_et_deposer(url: str, conn_id: str, stage: str) -> str:
        """Télécharge le fichier puis l'envoie sur le stage, dans la même tâche.

        Deux tâches distinctes ne partagent pas forcément le même disque : le
        fichier ne doit pas transiter d'une tâche à l'autre. Seul son nom passe
        à la tâche suivante.
        """
        destination = Path("/tmp") / url.rsplit("/", 1)[-1]
        try:
            with requests.get(url, stream=True, timeout=120) as reponse:
                reponse.raise_for_status()
                with destination.open("wb") as f:
                    for morceau in reponse.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(morceau)
            print(f"Téléchargé : {destination.name} ({destination.stat().st_size / 1e6:.1f} Mo)")

            with SnowflakeHook(snowflake_conn_id=conn_id).get_conn() as conn, conn.cursor() as cur:
                # Racine du stage, sans compression : _source_file contiendra
                # exactement le nom du fichier, dont les SQL fournis extraient AAAA-MM
                cur.execute(
                    f"PUT 'file://{destination}' @{stage} AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
                )
                print(f"PUT {destination.name} : {cur.fetchone()[6]}")  # UPLOADED ou SKIPPED
        finally:
            destination.unlink(missing_ok=True)  # ne pas remplir le disque du conteneur
        return destination.name

    @task
    def copier_dans_la_table(
        fichier: str, conn_id: str, stage: str, format_fichier: str, table: str
    ) -> int:
        """COPY INTO avec les mêmes options que ingestion/charger_mois.py."""
        with SnowflakeHook(snowflake_conn_id=conn_id).get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                COPY INTO {table}
                FROM @{stage}
                FILES = ('{fichier}')
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
                    print(f"COPY {resultat['file']} : {resultat['status']}, {resultat['rows_loaded']} lignes")
                    ajoutees += int(resultat["rows_loaded"] or 0)
                else:
                    # Fichier déjà chargé : « Copy executed with 0 files processed. »
                    print(f"COPY : {resultat.get('status', ligne)}")

            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE _source_file = %s", (fichier,))
            total = cur.fetchone()[0]

        print(f"Bilan : {ajoutees} lignes ajoutées ; {total} lignes de {fichier} dans {table}")
        if total == 0:
            raise ValueError(f"Aucune ligne de {fichier} dans {table}")
        return total

    # Les arguments en {{ ... }} sont rendus par Airflow avant l'exécution de la tâche
    url = verifier_disponibilite(url=URL)
    fichier = telecharger_et_deposer(url=url, conn_id="{{ params.conn_id }}", stage="{{ params.stage }}")
    copier_dans_la_table(
        fichier=fichier,
        conn_id="{{ params.conn_id }}",
        stage="{{ params.stage }}",
        format_fichier="{{ params.format_fichier }}",
        table="{{ params.table_brute }}",
    )


nyc_taxi_pipeline()