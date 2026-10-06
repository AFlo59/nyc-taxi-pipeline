-- La part des trajets écartés du mois reste sous le seuil du DAG
-- (params.max_pct_trajets_ecartes, en %). Au-delà, le fichier du mois est
-- probablement anormal : on s'arrête avant d'alimenter la table de faits.
-- Une seule ligne : si une valeur est fausse (ou NULL), la tâche échoue.
SELECT
    COUNT(*) > 0 AS mois_present,
    COUNT_IF(rejection_reason IS NOT NULL) * 100 / NULLIF(COUNT(*), 0)
        <= {{ params.max_pct_trajets_ecartes }} AS ecartes_sous_le_seuil
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;
