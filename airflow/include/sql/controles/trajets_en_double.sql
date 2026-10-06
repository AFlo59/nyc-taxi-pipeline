-- Aucun trajet en double dans la table de faits pour le mois traité :
-- trip_sk identifie un trajet (MD5 de sa clé métier), il doit être unique.
-- Une seule ligne : si une valeur est fausse, la tâche échoue.
SELECT
    COUNT(*) > 0                       AS mois_present,
    COUNT(*) = COUNT(DISTINCT trip_sk) AS aucun_trajet_en_double
FROM NYC_TAXI.MARTS.FCT_TRIPS
WHERE source_file_month = '{{ ds }}'::date;
