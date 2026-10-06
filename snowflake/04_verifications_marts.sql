-- =====================================================================
-- 04_verifications_marts.sql — Preuves des jours 4 et 5
-- ---------------------------------------------------------------------
-- Gabarit Jinja : python snowflake/rendre.py snowflake/04_verifications_marts.sql
--
-- À exécuter requête par requête (Ctrl+Entrée), dans une seule feuille
-- Snowsight : les variables SET restent dans la session de la feuille.
-- =====================================================================
USE ROLE {{ SNOWFLAKE_ROLE }};
USE WAREHOUSE {{ SNOWFLAKE_WAREHOUSE }};


-- ---------------------------------------------------------------------
-- 1. Choisir le seuil du contrôle « trop de trajets écartés » (jour 4)
--    Part des trajets écartés par mois : le seuil du DAG
--    (max_pct_trajets_ecartes, 10 par défaut) doit être au-dessus,
--    avec de la marge.
-- ---------------------------------------------------------------------
SELECT
    source_file_month                                   AS mois,
    COUNT(*)                                            AS nb_trajets,
    COUNT_IF(rejection_reason IS NOT NULL)              AS nb_ecartes,
    ROUND(nb_ecartes * 100 / nb_trajets, 2)             AS pct_ecartes
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------
-- 2. Rejouabilité : relancer février ne change aucun compte (jour 4)
-- ---------------------------------------------------------------------

-- 2.1 Nombre de lignes de chaque table. Colonne attendu : valeurs du README
--     du kit après les trois mois (vide quand le kit ne la donne pas).
SELECT 1 AS ordre, 'RAW.YELLOW_TRIPDATA' AS table_name, COUNT(*) AS nb_lignes, 11198026 AS attendu FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
UNION ALL SELECT 2,  'RAW.TAXI_ZONE_LOOKUP',              COUNT(*), 265      FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP
UNION ALL SELECT 3,  'STAGING.STG_TLC__YELLOW_TRIPS',     COUNT(*), 11198026 FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
UNION ALL SELECT 4,  'STAGING.STG_TLC__TAXI_ZONES',       COUNT(*), 265      FROM NYC_TAXI.STAGING.STG_TLC__TAXI_ZONES
UNION ALL SELECT 5,  'STAGING.PAYMENT_TYPE_CODES',        COUNT(*), 7        FROM NYC_TAXI.STAGING.PAYMENT_TYPE_CODES
UNION ALL SELECT 6,  'STAGING.RATE_CODE_CODES',           COUNT(*), 7        FROM NYC_TAXI.STAGING.RATE_CODE_CODES
UNION ALL SELECT 7,  'STAGING.VENDOR_CODES',              COUNT(*), 4        FROM NYC_TAXI.STAGING.VENDOR_CODES
UNION ALL SELECT 8,  'INTERMEDIATE.INT_TRIPS__FLAGGED',   COUNT(*), 11198026 FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
UNION ALL SELECT 9,  'INTERMEDIATE.INT_TRIPS__ENRICHED',  COUNT(*), 10382378 FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__ENRICHED
UNION ALL SELECT 10, 'MARTS.DIM_DATE',                    COUNT(*), 90       FROM NYC_TAXI.MARTS.DIM_DATE
UNION ALL SELECT 11, 'MARTS.DIM_PAYMENT_TYPE',            COUNT(*), 7        FROM NYC_TAXI.MARTS.DIM_PAYMENT_TYPE
UNION ALL SELECT 12, 'MARTS.DIM_RATE_CODE',               COUNT(*), 7        FROM NYC_TAXI.MARTS.DIM_RATE_CODE
UNION ALL SELECT 13, 'MARTS.DIM_VENDOR',                  COUNT(*), 4        FROM NYC_TAXI.MARTS.DIM_VENDOR
UNION ALL SELECT 14, 'MARTS.DIM_ZONE',                    COUNT(*), 265      FROM NYC_TAXI.MARTS.DIM_ZONE
UNION ALL SELECT 15, 'MARTS.FCT_TRIPS',                   COUNT(*), 10382378 FROM NYC_TAXI.MARTS.FCT_TRIPS
UNION ALL SELECT 16, 'MARTS.MART_DAILY_REVENUE',          COUNT(*), NULL     FROM NYC_TAXI.MARTS.MART_DAILY_REVENUE
UNION ALL SELECT 17, 'MARTS.MART_ZONE_HOURLY_DEMAND',     COUNT(*), 11524    FROM NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND
UNION ALL SELECT 18, 'MARTS.MART_DATA_QUALITY',           COUNT(*), 18       FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
ORDER BY ordre;

-- 2.2 Juste après 2.1 : mémoriser l'identifiant de ce résultat
SET comptes_avant = LAST_QUERY_ID();

-- 2.3 Dans Airflow : Runs → l'exécution de février → Clear, attendre la fin.
--     Puis réexécuter la requête 2.1, et tout de suite après :
SET comptes_apres = LAST_QUERY_ID();

-- 2.4 Comparaison : identique = TRUE sur toutes les lignes
SELECT
    av.table_name,
    av.nb_lignes                 AS avant,
    ap.nb_lignes                 AS apres,
    av.nb_lignes = ap.nb_lignes  AS identique
FROM TABLE(RESULT_SCAN($comptes_avant)) av
JOIN TABLE(RESULT_SCAN($comptes_apres)) ap ON ap.table_name = av.table_name
ORDER BY av.ordre;

-- 2.5 Février a bien été recalculé (horodatage récent), sans doublon
SELECT source_file_month, COUNT(*) AS nb_trajets, COUNT(DISTINCT trip_sk) AS nb_trip_sk,
       MAX(loaded_at) AS charge_dans_raw_le
FROM NYC_TAXI.MARTS.FCT_TRIPS
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------
-- 3. Compter soi-même les trajets anormaux d'un mois (jour 5)
--    Chaque règle de int_trips__flagged.sql est comptée seule, directement
--    sur RAW. Une ligne peut enfreindre plusieurs règles : MART_DATA_QUALITY
--    ne garde que la première, d'où les écarts règle par règle. Le total
--    « au moins une règle » doit, lui, tomber exactement juste.
-- ---------------------------------------------------------------------
SET mois = '2025-01-01';

WITH trajets AS (
    SELECT
        tpep_pickup_datetime   AS prise,
        tpep_dropoff_datetime  AS depose,
        trip_distance::float   AS distance,
        fare_amount::float     AS tarif,
        total_amount::float    AS total,
        pulocationid           AS zone_prise,
        dolocationid           AS zone_depose
    FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
    WHERE _source_file = 'yellow_tripdata_' || TO_CHAR($mois::date, 'YYYY-MM') || '.parquet'
),
comptes AS (
    SELECT
        COUNT(*)                                                            AS total_lignes,
        COUNT_IF(prise IS NULL OR depose IS NULL)                           AS timestamp_null,
        COUNT_IF(depose <= prise)                                           AS duration_non_positive,
        COUNT_IF(DATEDIFF('second', prise, depose) > 180 * 60)              AS duration_too_long,
        COUNT_IF(DATE_TRUNC('month', prise) <> $mois::date)                 AS pickup_outside_file_month,
        COUNT_IF(distance <= 0 OR distance > 100)                           AS distance_out_of_range,
        COUNT_IF(tarif < 0 OR total <= 0)                                   AS amount_non_positive,
        COUNT_IF(zone_prise IS NULL OR zone_depose IS NULL)                 AS zone_null,
        COUNT_IF(prise IS NULL OR depose IS NULL OR depose <= prise
                 OR DATEDIFF('second', prise, depose) > 180 * 60
                 OR DATE_TRUNC('month', prise) <> $mois::date
                 OR distance <= 0 OR distance > 100
                 OR tarif < 0 OR total <= 0
                 OR zone_prise IS NULL OR zone_depose IS NULL)              AS au_moins_une_regle
    FROM trajets
),
mes_comptes AS (
              SELECT 1 AS ordre, 'timestamp_null' AS regle, timestamp_null AS mon_comptage FROM comptes
    UNION ALL SELECT 2, 'duration_non_positive',     duration_non_positive     FROM comptes
    UNION ALL SELECT 3, 'duration_too_long',         duration_too_long         FROM comptes
    UNION ALL SELECT 4, 'pickup_outside_file_month', pickup_outside_file_month FROM comptes
    UNION ALL SELECT 5, 'distance_out_of_range',     distance_out_of_range     FROM comptes
    UNION ALL SELECT 6, 'amount_non_positive',       amount_non_positive       FROM comptes
    UNION ALL SELECT 7, 'zone_null',                 zone_null                 FROM comptes
    UNION ALL SELECT 8, 'au moins une règle',        au_moins_une_regle        FROM comptes
    UNION ALL SELECT 9, 'valid',                     total_lignes - au_moins_une_regle FROM comptes
),
mart AS (
    SELECT status AS regle, nb_rows FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
    WHERE source_file_month = $mois::date
    UNION ALL
    SELECT 'au moins une règle', SUM(nb_rows) FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
    WHERE source_file_month = $mois::date AND status <> 'valid'
)
SELECT
    c.regle,
    c.mon_comptage,
    COALESCE(m.nb_rows, 0)                   AS mart_data_quality,
    c.mon_comptage - COALESCE(m.nb_rows, 0)  AS ecart
FROM mes_comptes c
LEFT JOIN mart m ON m.regle = c.regle
ORDER BY c.ordre;

-- 3.2 Trajets valides (MART_DATA_QUALITY) contre trajets de FCT_TRIPS :
--     la différence = trajets valides retirés comme doublons par int_trips__enriched
SELECT
    q.source_file_month                    AS mois,
    q.nb_rows                              AS trajets_valides,
    f.nb_trajets                           AS trajets_fct_trips,
    q.nb_rows - f.nb_trajets               AS doublons_retires
FROM NYC_TAXI.MARTS.MART_DATA_QUALITY q
JOIN (SELECT source_file_month, COUNT(*) AS nb_trajets
      FROM NYC_TAXI.MARTS.FCT_TRIPS GROUP BY 1) f
  ON f.source_file_month = q.source_file_month
WHERE q.status = 'valid'
ORDER BY 1;


-- ---------------------------------------------------------------------
-- 4. La réponse à la direction (jour 5, docs/REPONSE.md)
--    Où et quand la demande est la plus forte, et combien rapporte un
--    trajet selon la zone, l'heure et le mode de paiement.
-- ---------------------------------------------------------------------

-- 4.1 Les 10 créneaux zone × heure les plus demandés (janvier à mars 2025)
WITH periode AS (
    SELECT COUNT(*) AS nb_jours FROM NYC_TAXI.MARTS.DIM_DATE
)
SELECT
    z.zone_name                                                   AS zone,
    z.borough                                                     AS arrondissement,
    f.pickup_hour                                                 AS heure,
    COUNT(*)                                                      AS nb_trajets,
    ROUND(COUNT(*) / ANY_VALUE(p.nb_jours), 1)                    AS trajets_par_jour,
    ROUND(AVG(f.total_amount), 2)                                 AS revenu_moyen_usd,
    ROUND(AVG(IFF(f.payment_type_key = 1, f.total_amount, NULL)), 2) AS revenu_moyen_carte_usd,
    ROUND(AVG(IFF(f.payment_type_key = 2, f.total_amount, NULL)), 2) AS revenu_moyen_especes_usd,
    ROUND(COUNT_IF(f.payment_type_key = 1) * 100 / COUNT(*), 1)  AS pct_paiement_carte
FROM NYC_TAXI.MARTS.FCT_TRIPS f
JOIN NYC_TAXI.MARTS.DIM_ZONE z ON z.zone_key = f.pickup_zone_key
CROSS JOIN periode p
WHERE NOT z.is_unknown_zone
GROUP BY 1, 2, 3
ORDER BY nb_trajets DESC
LIMIT 10;

-- 4.2 Combien rapporte un trajet selon le mode de paiement (toute la période)
SELECT
    p.payment_type_label                         AS mode_de_paiement,
    COUNT(*)                                     AS nb_trajets,
    ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER (), 1) AS pct_trajets,
    ROUND(AVG(f.total_amount), 2)                AS revenu_moyen_usd,
    ROUND(AVG(f.fare_amount), 2)                 AS tarif_moyen_usd,
    ROUND(AVG(f.tip_amount), 2)                  AS pourboire_moyen_usd
FROM NYC_TAXI.MARTS.FCT_TRIPS f
LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p ON p.payment_type_key = f.payment_type_key
GROUP BY 1
ORDER BY nb_trajets DESC;

-- 4.3 Les zones où un trajet rapporte le plus (au moins 30 trajets par jour)
SELECT
    pickup_zone_name                                AS zone,
    pickup_borough                                  AS arrondissement,
    SUM(nb_trips)                                   AS nb_trajets,
    ROUND(SUM(total_revenue) / SUM(nb_trips), 2)    AS revenu_moyen_usd
FROM NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND
WHERE pickup_zone_key NOT IN (264, 265)
GROUP BY 1, 2
HAVING SUM(nb_trips) >= 30 * (SELECT COUNT(*) FROM NYC_TAXI.MARTS.DIM_DATE)
ORDER BY revenu_moyen_usd DESC
LIMIT 10;
