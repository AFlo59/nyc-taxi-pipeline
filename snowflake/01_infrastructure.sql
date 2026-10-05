-- =====================================================================
-- 01_infrastructure.sql — Entrepôt Snowflake NYC Yellow Taxi
-- ---------------------------------------------------------------------
-- Crée le rôle des outils, le warehouse, la base, les quatre schémas,
-- les droits minimaux, l'utilisateur de service et le plafond de coûts.
--
-- Gabarit Jinja : les variables entre doubles accolades sont remplacées
-- par les valeurs du .env :
--   python snowflake/rendre.py snowflake/01_infrastructure.sql
-- puis exécuter snowflake/build/01_infrastructure.sql dans Snowsight,
-- en entier (Run All), avec un utilisateur administrateur.
--
-- Rejouable : chaque création utilise IF NOT EXISTS.
-- Noms imposés par CONTRAT_RAW.md (base, schémas) : écrits en dur.
-- Noms libres (rôle, warehouse, utilisateur, plafond) : variables.
-- Ordre : rôle → warehouse → base → schémas → droits → utilisateur → coûts
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Rôle des outils (USERADMIN crée les rôles et les utilisateurs)
-- ---------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS {{ SNOWFLAKE_ROLE }}
    COMMENT = 'Rôle des outils (script Python, Airflow) : charge RAW, construit STAGING, INTERMEDIATE et MARTS';

-- Rattachement à la hiérarchie : SYSADMIN hérite des droits du rôle
-- et peut voir et gérer les objets qu'il créera.
GRANT ROLE {{ SNOWFLAKE_ROLE }} TO ROLE SYSADMIN;


-- ---------------------------------------------------------------------
-- 2. Warehouse, base et schémas (SYSADMIN crée les objets)
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS {{ SNOWFLAKE_WAREHOUSE }}
    WAREHOUSE_SIZE = 'XSMALL'                -- plus petite taille : 1 crédit par heure d'activité
    AUTO_SUSPEND = 60                        -- suspendu après 60 s sans requête
    AUTO_RESUME = TRUE                       -- redémarre seul à la requête suivante
    INITIALLY_SUSPENDED = TRUE               -- ne consomme rien à la création
    STATEMENT_TIMEOUT_IN_SECONDS = 3600      -- garde-fou : aucune requête ne tourne plus d'une heure
    COMMENT = 'Calcul du pipeline NYC Taxi (chargement et transformations), taille XS';

CREATE DATABASE IF NOT EXISTS NYC_TAXI
    COMMENT = 'Entrepôt NYC Yellow Taxi (Hudson Cab Partners) : couches RAW, STAGING, INTERMEDIATE, MARTS';

-- Snowflake crée un schéma PUBLIC avec chaque base : inutile ici, le
-- contrat prévoit exactement quatre schémas.
DROP SCHEMA IF EXISTS NYC_TAXI.PUBLIC;

CREATE SCHEMA IF NOT EXISTS NYC_TAXI.RAW
    COMMENT = 'Données brutes : copie fidèle des fichiers TLC, avec le stage et les formats de fichier';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.STAGING
    COMMENT = 'Données renommées et typées : vues sur RAW et tables de codes';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.INTERMEDIATE
    COMMENT = 'Données nettoyées : trajets étiquetés, puis trajets valides enrichis';
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.MARTS
    COMMENT = 'Données prêtes à analyser : dimensions, table de faits et tables d''analyse';


-- ---------------------------------------------------------------------
-- 3. Droits du rôle des outils (moindre privilège)
-- SYSADMIN est propriétaire des objets ci-dessus : il peut donner des
-- droits dessus. Le rôle des outils sera propriétaire de tout ce qu'il
-- créera lui-même, sans droit supplémentaire à prévoir.
-- ---------------------------------------------------------------------

-- Calcul : USAGE suffit, AUTO_RESUME redémarre le warehouse.
-- Pas d'OPERATE ni de MODIFY : l'outil ne suspend ni ne redimensionne rien.
GRANT USAGE ON WAREHOUSE {{ SNOWFLAKE_WAREHOUSE }} TO ROLE {{ SNOWFLAKE_ROLE }};

-- Entrée dans la base. Pas de CREATE SCHEMA : les schémas sont imposés.
GRANT USAGE ON DATABASE NYC_TAXI TO ROLE {{ SNOWFLAKE_ROLE }};

-- RAW : formats de fichier, stage et tables pour le chargement
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA NYC_TAXI.RAW TO ROLE {{ SNOWFLAKE_ROLE }};

-- STAGING, INTERMEDIATE, MARTS : tables et vues des fichiers SQL fournis
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.STAGING      TO ROLE {{ SNOWFLAKE_ROLE }};
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.INTERMEDIATE TO ROLE {{ SNOWFLAKE_ROLE }};
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI.MARTS        TO ROLE {{ SNOWFLAKE_ROLE }};


-- ---------------------------------------------------------------------
-- 4. Utilisateur de service (USERADMIN)
-- TYPE = SERVICE : pas de mot de passe, pas d'accès à Snowsight,
-- authentification par paire de clés uniquement.
-- ---------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS {{ SNOWFLAKE_USER }}
    TYPE = SERVICE
    DEFAULT_ROLE = {{ SNOWFLAKE_ROLE }}
    DEFAULT_WAREHOUSE = {{ SNOWFLAKE_WAREHOUSE }}
    DEFAULT_NAMESPACE = NYC_TAXI.RAW
    COMMENT = 'Utilisateur de service du script Python et d''Airflow, authentifié par paire de clés';

-- DEFAULT_ROLE ne donne pas le rôle : ce GRANT est obligatoire.
GRANT ROLE {{ SNOWFLAKE_ROLE }} TO USER {{ SNOWFLAKE_USER }};

{% if SNOWFLAKE_RSA_PUBLIC_KEY %}
-- Clé publique, lue par rendre.py dans le fichier SNOWFLAKE_PUBLIC_KEY_PATH.
-- La clé privée (rsa_key.p8) ne va JAMAIS dans Git ni dans ce script.
ALTER USER {{ SNOWFLAKE_USER }} SET RSA_PUBLIC_KEY = '{{ SNOWFLAKE_RSA_PUBLIC_KEY }}';
{% else %}
-- Clé publique introuvable au moment du rendu : la générer (README),
-- renseigner SNOWFLAKE_PUBLIC_KEY_PATH dans le .env, puis relancer rendre.py.
{% endif %}


-- ---------------------------------------------------------------------
-- 5. Plafond de coûts (ACCOUNTADMIN, seul rôle autorisé)
-- Quota mensuel de crédits sur le warehouse : alerte à 80 %,
-- suspension à 100 %. Largement au-dessus du besoin du pipeline.
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS {{ SNOWFLAKE_RESOURCE_MONITOR }}
    WITH CREDIT_QUOTA = {{ SNOWFLAKE_CREDIT_QUOTA }}
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE {{ SNOWFLAKE_WAREHOUSE }} SET RESOURCE_MONITOR = {{ SNOWFLAKE_RESOURCE_MONITOR }};
