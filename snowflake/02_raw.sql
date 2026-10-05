-- =====================================================================
-- 02_raw.sql — Couche RAW : formats de fichier, stage, tables
-- ---------------------------------------------------------------------
-- À exécuter APRÈS 01_infrastructure.sql, en entier (Run All), avec le
-- rôle des outils : le rôle qui crée un objet en devient propriétaire,
-- et le contrat exige que les tables RAW appartiennent au rôle des outils.
--
-- Gabarit Jinja : python snowflake/rendre.py snowflake/02_raw.sql
-- Noms des tables et des colonnes imposés par CONTRAT_RAW.md : en dur.
-- =====================================================================

USE ROLE {{ SNOWFLAKE_ROLE }};
USE WAREHOUSE {{ SNOWFLAKE_WAREHOUSE }};
USE SCHEMA NYC_TAXI.RAW;


-- ---------------------------------------------------------------------
-- 1. Formats de fichier
-- ---------------------------------------------------------------------

-- Trajets : le Parquet contient déjà les noms et les types des colonnes.
CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.PARQUET_FF
    TYPE = PARQUET
    COMMENT = 'Fichiers mensuels des trajets TLC (yellow_tripdata_AAAA-MM.parquet)';

-- Zones : la ligne d'en-tête est lue pour charger par nom de colonne.
CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.CSV_FF
    TYPE = CSV
    PARSE_HEADER = TRUE                      -- indispensable pour MATCH_BY_COLUMN_NAME
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'       -- valeurs entre guillemets dans le fichier
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE   -- la table a 2 colonnes techniques de plus que le fichier
    COMMENT = 'Liste des zones TLC (taxi_zone_lookup.csv)';


-- ---------------------------------------------------------------------
-- 2. Stage interne
-- Les fichiers sont déposés à la racine (PUT ... @TLC_STAGE, sans
-- sous-dossier) : _source_file contient alors exactement le nom du
-- fichier, dont les fichiers SQL fournis extraient le mois AAAA-MM.
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS NYC_TAXI.RAW.TLC_STAGE
    COMMENT = 'Dépôt des fichiers TLC (trajets et zones) avant COPY INTO';


-- ---------------------------------------------------------------------
-- 3. Tables
-- Noms sans guillemets : Snowflake les stocke en majuscules, et les
-- fichiers SQL fournis les retrouvent sans tenir compte de la casse.
--
-- Pas de DEFAULT sur les colonnes techniques : COPY INTO les remplit
-- avec INCLUDE_METADATA (section 4), seule source de ces valeurs.
-- ---------------------------------------------------------------------

-- Trajets. Types à précision explicite, avec une marge sur les valeurs
-- observées en janvier 2025 : une seule valeur hors limite ferait échouer
-- le chargement du mois entier (ON_ERROR = ABORT_STATEMENT).
--   NUMBER(3,0)       codes et compteurs (fournisseur, tarif, paiement, passagers)
--   NUMBER(38,0)      identifiants de zone
--   NUMBER(10,2)      montants et distances : centimes exacts, jusqu'à 99 999 999,99
--   VARCHAR(n)        texte
--   TIMESTAMP_NTZ(9)  dates et heures sans fuseau (heure locale de New York)
--
-- Pas de NOT NULL sur les colonnes de données : les fichiers réels
-- contiennent des valeurs vides, et une seule ferait échouer le
-- chargement du mois entier. Les anomalies se traitent en INTERMEDIATE.
--
-- Pas de clé primaire : les fichiers TLC n'ont aucun identifiant de
-- trajet, et un même trajet peut apparaître deux fois (dédoublonné en
-- aval grâce à _loaded_at).
--
-- Pas de CLUSTER BY : environ 11 millions de lignes sur trois mois, loin
-- des tables de plusieurs To où le clustering devient utile. Les fichiers
-- étant chargés mois par mois, les micro-partitions sont déjà rangées par
-- mois. Le reclustering automatique consommerait des crédits sans gain.
CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.YELLOW_TRIPDATA (
    vendorid               NUMBER(3,0)       COMMENT 'Fournisseur du système d''enregistrement (TPEP) : 1 = Creative Mobile Technologies, 2 = Curb Mobility, 6 = Myle Technologies, 7 = Helix',
    tpep_pickup_datetime   TIMESTAMP_NTZ(9)  COMMENT 'Date et heure de prise en charge (compteur enclenché), heure locale de New York',
    tpep_dropoff_datetime  TIMESTAMP_NTZ(9)  COMMENT 'Date et heure de dépose (compteur arrêté), heure locale de New York',
    passenger_count        NUMBER(3,0)       COMMENT 'Nombre de passagers, saisi par le chauffeur',
    trip_distance          NUMBER(10,2)      COMMENT 'Distance parcourue en miles, mesurée par le taximètre',
    ratecodeid             NUMBER(3,0)       COMMENT 'Tarif appliqué en fin de trajet : 1 = standard, 2 = JFK, 3 = Newark, 4 = Nassau ou Westchester, 5 = négocié, 6 = trajet partagé, 99 = inconnu',
    store_and_fwd_flag     VARCHAR(10)       COMMENT 'Y = trajet stocké dans le véhicule faute de connexion puis transmis plus tard, N = transmis directement',
    pulocationid           NUMBER(38,0)      COMMENT 'Zone TLC de prise en charge (jointure avec TAXI_ZONE_LOOKUP.locationid)',
    dolocationid           NUMBER(38,0)      COMMENT 'Zone TLC de dépose (jointure avec TAXI_ZONE_LOOKUP.locationid)',
    payment_type           NUMBER(3,0)       COMMENT 'Mode de paiement : 0 = Flex Fare, 1 = carte bancaire, 2 = espèces, 3 = gratuit, 4 = litige, 5 = inconnu, 6 = trajet annulé',
    fare_amount            NUMBER(10,2)      COMMENT 'Prix de la course calculé par le taximètre (temps et distance), en dollars',
    extra                  NUMBER(10,2)      COMMENT 'Suppléments divers (heures de pointe, nuit), en dollars',
    mta_tax                NUMBER(10,2)      COMMENT 'Taxe MTA déclenchée automatiquement selon le tarif, en dollars',
    tip_amount             NUMBER(10,2)      COMMENT 'Pourboire, rempli automatiquement pour les paiements par carte (pourboires en espèces non inclus), en dollars',
    tolls_amount           NUMBER(10,2)      COMMENT 'Total des péages payés pendant le trajet, en dollars',
    improvement_surcharge  NUMBER(10,2)      COMMENT 'Supplément d''amélioration perçu à la prise en charge, en dollars',
    total_amount           NUMBER(10,2)      COMMENT 'Montant total facturé au passager, hors pourboires en espèces, en dollars',
    congestion_surcharge   NUMBER(10,2)      COMMENT 'Supplément de congestion de l''État de New York, en dollars',
    airport_fee            NUMBER(10,2)      COMMENT 'Frais de prise en charge aux aéroports JFK et LaGuardia, en dollars',
    cbd_congestion_fee     NUMBER(10,2)      COMMENT 'Redevance de la zone de péage urbain de Manhattan (MTA), en vigueur depuis janvier 2025, en dollars',
    _source_file           VARCHAR(100)      COMMENT 'Colonne technique : nom du fichier chargé, par exemple yellow_tripdata_2025-01.parquet (rempli par COPY INTO)',
    _loaded_at             TIMESTAMP_NTZ(9)  COMMENT 'Colonne technique : date et heure du chargement, sert à garder la première version d''un trajet en double (rempli par COPY INTO)'
)
COMMENT = 'Trajets des taxis jaunes : copie fidèle des fichiers Parquet mensuels TLC, une ligne par trajet';


-- Zones : fichier de référence fixe, tailles mesurées sur le fichier
-- (MAX(LENGTH) par colonne). Clé primaire déclarée pour documenter
-- l'unicité exigée par le contrat ; Snowflake ne l'applique pas : elle
-- est vérifiée dans 03_verifications.sql.
CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.TAXI_ZONE_LOOKUP (
    locationid    NUMBER(3,0)       COMMENT 'Identifiant de la zone TLC (1 à 265), clé de jointure avec pulocationid et dolocationid',
    borough       VARCHAR(20)       COMMENT 'Arrondissement : Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR (aéroport de Newark) ou inconnu',
    zone          VARCHAR(50)       COMMENT 'Nom de la zone (quartier, aéroport)',
    service_zone  VARCHAR(12)       COMMENT 'Type de zone de service : Yellow Zone, Boro Zone, Airports, EWR',
    _source_file  VARCHAR(20)       COMMENT 'Colonne technique : nom du fichier chargé (taxi_zone_lookup.csv), rempli par COPY INTO',
    _loaded_at    TIMESTAMP_NTZ(9)  COMMENT 'Colonne technique : date et heure du chargement, rempli par COPY INTO',
    CONSTRAINT PK_TAXI_ZONE_LOOKUP PRIMARY KEY (locationid)
)
COMMENT = 'Liste des 265 zones TLC : arrondissement, nom de zone et type de zone de service';


-- ---------------------------------------------------------------------
-- 4. Chargement (référence, exécuté par ingestion/charger_mois.py et
-- par le DAG Airflow). PUT ne fonctionne pas dans Snowsight : le script
-- dépose le fichier sur le stage, puis lance le COPY INTO.
--
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE absorbe les variations entre
-- mois : casse différente d'un nom de colonne (Airport_fee / airport_fee),
-- colonne absente d'un mois (laissée vide).
--
-- Rejouable : Snowflake mémorise pendant 64 jours les fichiers déjà
-- chargés dans chaque table et ne les recharge pas. Ne jamais utiliser
-- FORCE = TRUE. Pour vider une table et recharger : TRUNCATE, pas DELETE.
-- ---------------------------------------------------------------------
-- PUT file:///chemin/yellow_tripdata_2025-01.parquet @NYC_TAXI.RAW.TLC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=FALSE;
--
-- COPY INTO NYC_TAXI.RAW.YELLOW_TRIPDATA
--     FROM @NYC_TAXI.RAW.TLC_STAGE
--     FILES = ('yellow_tripdata_2025-01.parquet')
--     FILE_FORMAT = (FORMAT_NAME = NYC_TAXI.RAW.PARQUET_FF)
--     MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
--     INCLUDE_METADATA = (_source_file = METADATA$FILENAME, _loaded_at = METADATA$START_SCAN_TIME)
--     ON_ERROR = ABORT_STATEMENT;
--
-- COPY INTO NYC_TAXI.RAW.TAXI_ZONE_LOOKUP
--     FROM @NYC_TAXI.RAW.TLC_STAGE
--     FILES = ('taxi_zone_lookup.csv')
--     FILE_FORMAT = (FORMAT_NAME = NYC_TAXI.RAW.CSV_FF)
--     MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
--     INCLUDE_METADATA = (_source_file = METADATA$FILENAME, _loaded_at = METADATA$START_SCAN_TIME)
--     ON_ERROR = ABORT_STATEMENT;
