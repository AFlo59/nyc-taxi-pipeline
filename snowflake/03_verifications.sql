-- =====================================================================
-- 03_verifications.sql — Preuves pour la démonstration
-- ---------------------------------------------------------------------
-- Gabarit Jinja : python snowflake/rendre.py snowflake/03_verifications.sql
--
-- Ne PAS lancer en Run All : les requêtes de la section 2 échouent
-- exprès et arrêteraient le script. Exécuter requête par requête
-- (Ctrl+Entrée sur chaque ligne).
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Compte, droits du rôle des outils (capture « droits du rôle »)
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

-- Identifiant de compte à renseigner dans SNOWFLAKE_ACCOUNT (.env)
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;

SHOW GRANTS TO ROLE {{ SNOWFLAKE_ROLE }};        -- tous les droits du rôle, et rien de plus
SHOW GRANTS OF ROLE {{ SNOWFLAKE_ROLE }};        -- à qui il est donné : SYSADMIN et {{ SNOWFLAKE_USER }}
SHOW GRANTS TO USER {{ SNOWFLAKE_USER }};        -- les rôles de l'utilisateur de service
DESC USER {{ SNOWFLAKE_USER }};                  -- TYPE = SERVICE, ligne RSA_PUBLIC_KEY_FP renseignée


-- ---------------------------------------------------------------------
-- 2. Accès refusés hors périmètre (chaque requête doit ÉCHOUER)
-- ---------------------------------------------------------------------
USE ROLE {{ SNOWFLAKE_ROLE }};
USE WAREHOUSE {{ SNOWFLAKE_WAREHOUSE }};

CREATE SCHEMA NYC_TAXI.HORS_PERIMETRE;                                      -- pas de CREATE SCHEMA : schémas imposés
CREATE DATABASE HORS_PERIMETRE_DB;                                          -- pas de CREATE DATABASE
CREATE WAREHOUSE HORS_PERIMETRE_WH;                                         -- pas de CREATE WAREHOUSE
ALTER WAREHOUSE {{ SNOWFLAKE_WAREHOUSE }} SET WAREHOUSE_SIZE = 'LARGE';     -- pas de MODIFY : la taille XS est protégée
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1;   -- facturation réservée à ACCOUNTADMIN


-- ---------------------------------------------------------------------
-- 3. Contrat de la couche RAW
-- ---------------------------------------------------------------------
USE ROLE {{ SNOWFLAKE_ROLE }};
USE WAREHOUSE {{ SNOWFLAKE_WAREHOUSE }};
USE SCHEMA NYC_TAXI.RAW;

-- Colonne owner = {{ SNOWFLAKE_ROLE }} pour les tables, le stage et les formats
SHOW TABLES IN SCHEMA NYC_TAXI.RAW;
SHOW STAGES IN SCHEMA NYC_TAXI.RAW;
SHOW FILE FORMATS IN SCHEMA NYC_TAXI.RAW;

-- Lignes par fichier : 3 475 226 pour janvier 2025, colonnes techniques remplies.
-- Relancer ensuite le chargement d'un mois : ce compte ne doit pas changer.
SELECT _source_file, COUNT(*) AS nb_lignes, MIN(_loaded_at) AS charge_le
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
GROUP BY 1
ORDER BY 1;

-- Les montants ont gardé leurs décimales
SELECT fare_amount, total_amount FROM NYC_TAXI.RAW.YELLOW_TRIPDATA LIMIT 5;

-- 265 zones
SELECT COUNT(*) FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP;

-- Identifiant de zone unique (clé primaire non appliquée par Snowflake) :
-- aucune ligne attendue
SELECT locationid, COUNT(*) AS nb
FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP
GROUP BY 1
HAVING COUNT(*) > 1;

-- Zones de prise en charge ou de dépose absentes de TAXI_ZONE_LOOKUP.
-- Aucune ligne attendue ; sinon, anomalie à noter dans la fiche source.
SELECT 'pulocationid' AS colonne, t.pulocationid AS zone_inconnue, COUNT(*) AS nb_trajets
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA t
LEFT JOIN NYC_TAXI.RAW.TAXI_ZONE_LOOKUP z ON z.locationid = t.pulocationid
WHERE t.pulocationid IS NOT NULL AND z.locationid IS NULL
GROUP BY 1, 2
UNION ALL
SELECT 'dolocationid', t.dolocationid, COUNT(*)
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA t
LEFT JOIN NYC_TAXI.RAW.TAXI_ZONE_LOOKUP z ON z.locationid = t.dolocationid
WHERE t.dolocationid IS NOT NULL AND z.locationid IS NULL
GROUP BY 1, 2;

-- Historique de chargement (capture « historique de chargement Snowflake ») :
-- chaque fichier n'apparaît qu'une fois avec le statut Loaded.
SELECT file_name, status, row_count, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'YELLOW_TRIPDATA',
    START_TIME => DATEADD(DAY, -14, CURRENT_TIMESTAMP())))
ORDER BY last_load_time;


-- ---------------------------------------------------------------------
-- 4. Crédits consommés (capture « suivi des crédits »)
-- Réservé à ACCOUNTADMIN ; les chiffres ont jusqu'à 3 h de retard.
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

SELECT warehouse_name, DATE(start_time) AS jour, ROUND(SUM(credits_used), 3) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2;

SHOW RESOURCE MONITORS LIKE '{{ SNOWFLAKE_RESOURCE_MONITOR }}';   -- quota, crédits utilisés, seuils
