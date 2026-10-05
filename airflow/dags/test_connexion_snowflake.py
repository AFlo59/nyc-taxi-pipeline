"""Prouve qu'Airflow se connecte à Snowflake avec l'utilisateur de service.

La connexion snowflake_nyc_taxi vient de la variable AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI
du fichier airflow/.env (ignoré par Git) : aucune clé dans le code ni dans l'image.

Ce DAG n'a pas de planification : on le lance avec le bouton Trigger.
"""

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

CONN_ID = "snowflake_nyc_taxi"


@dag(
    dag_id="test_connexion_snowflake",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test"],
)
def test_connexion_snowflake():
    @task
    def qui_suis_je() -> None:
        utilisateur, role, warehouse = SnowflakeHook(snowflake_conn_id=CONN_ID).get_first(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
        )
        print(f"Utilisateur : {utilisateur} | rôle : {role} | warehouse : {warehouse}")
        # Contrainte du brief : les outils n'utilisent jamais ACCOUNTADMIN
        if role == "ACCOUNTADMIN":
            raise ValueError("Airflow ne doit jamais se connecter avec ACCOUNTADMIN")

    qui_suis_je()


test_connexion_snowflake()
