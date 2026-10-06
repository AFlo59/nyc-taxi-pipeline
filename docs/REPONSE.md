# Réponse à la direction d'Hudson Cab Partners

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## Le périmètre

- **Données** : les trajets valides de `NYC_TAXI.MARTS.FCT_TRIPS`, soit 10 382 378 trajets, après nettoyage et dédoublonnage par le pipeline.
- **Période** : de janvier à mars 2025, soit 90 jours.
- **Zone** : la zone de prise en charge.
- **Heure** : l'heure locale de New York.
- **Exclusions** : les zones inconnues de la TLC (264 et 265).

## La requête

Elle est aussi dans `snowflake/04_verifications_marts.sql`, section 4.1.

```sql
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
```

| Colonne | Ce qu'elle mesure |
|---|---|
| `nb_trajets` | trajets partis de la zone à cette heure, sur les 90 jours |
| `trajets_par_jour` | la même demande ramenée à une journée moyenne |
| `revenu_moyen_usd` | montant moyen payé par le passager (`total_amount`), tous modes de paiement confondus |
| `revenu_moyen_carte_usd`, `revenu_moyen_especes_usd` | le même montant, pour les paiements par carte (code 1) et en espèces (code 2) |
| `pct_paiement_carte` | part des trajets payés par carte |

## Le résultat : les 10 premiers créneaux zone × heure

Heure de prise en charge, heure locale de New York : « 18 h » = de 18 h 00 à 18 h 59. Classement par nombre de trajets, du 1er janvier au 31 mars 2025 (90 jours).

| Rang | Zone | Arrondissement | Heure | Trajets | Trajets par jour | Revenu moyen ($) | Carte ($) | Espèces ($) | % carte |
|---|---|---|---|---|---|---|---|---|---|
| 1 | Midtown Center | Manhattan | 18 h | 45 978 | 510,9 | 24,40 | 25,06 | 19,93 | 83,2 |
| 2 | Midtown Center | Manhattan | 17 h | 45 063 | 500,7 | 28,57 | 26,35 | 20,74 | 84,4 |
| 3 | Midtown Center | Manhattan | 19 h | 38 770 | 430,8 | 23,62 | 24,25 | 19,36 | 81,7 |
| 4 | Midtown Center | Manhattan | 20 h | 38 518 | 428,0 | 22,67 | 23,61 | 18,61 | 72,3 |
| 5 | Midtown Center | Manhattan | 16 h | 36 702 | 407,8 | 25,77 | 26,53 | 21,27 | 83,6 |
| 6 | Upper East Side North | Manhattan | 15 h | 36 598 | 406,6 | 20,37 | 20,64 | 17,12 | 81,3 |
| 7 | Upper East Side South | Manhattan | 14 h | 36 482 | 405,4 | 19,98 | 20,41 | 16,85 | 81,8 |
| 8 | Upper East Side South | Manhattan | 15 h | 36 399 | 404,4 | 19,98 | 20,34 | 16,94 | 82,4 |
| 9 | Times Sq/Theatre District | Manhattan | 21 h | 36 340 | 403,8 | 23,58 | 24,50 | 18,94 | 71,2 |
| 10 | Upper East Side South | Manhattan | 18 h | 36 093 | 401,0 | 21,23 | 21,54 | 17,97 | 81,9 |

## Ce qu'il faut en retenir

1. **Où et quand** : la demande se concentre à Manhattan, où se trouvent les dix créneaux les plus chargés. Midtown Center en occupe cinq, de 16 h à 21 h, avec un pic à 18 h : 511 courses par jour en moyenne. L'Upper East Side domine l'après-midi (14 h à 16 h), et Times Square le soir à 21 h.
2. **Combien rapporte un trajet** : dans ces créneaux, une course rapporte en moyenne de 20 à 29 $. Les plus rentables partent de Midtown Center en fin d'après-midi (28,57 $ à 17 h), les moins rentables de l'Upper East Side (environ 20 $).
3. **Mode de paiement** : 71 à 84 % des courses sont payées par carte, moins le soir (72 % à 20 h, 71 % à 21 h). Une course payée par carte rapporte 3,40 à 5,60 $ de plus qu'en espèces (20 à 27 $ contre 17 à 21 $), mais une partie de cet écart vient des pourboires, enregistrés seulement pour la carte.

## Les limites

- **Trois mois d'hiver.** La demande change avec les saisons (tourisme d'été, fêtes de fin d'année). Le classement est à confirmer sur une année complète.
- **Trajets écartés.** 815 648 lignes de RAW sur 11 198 026, soit 7,3 %, ne sont pas dans `FCT_TRIPS`. Ce sont des trajets écartés par les règles de nettoyage (distance nulle, montant négatif, durée impossible) ou des doublons. Le détail par raison est dans `MART_DATA_QUALITY`.
- **Toute la ville, pas seulement la flotte d'Hudson Cab.** Les fichiers de la TLC couvrent tous les taxis jaunes de New York, sans dire à quelle compagnie appartient chaque taxi. Le résultat décrit le marché, pas l'activité des 180 taxis.
- **Demande servie seulement.** On ne voit que les trajets réalisés. Les clients qui n'ont pas trouvé de taxi n'apparaissent pas, ni les VTC (Uber, Lyft), qui sont dans d'autres fichiers TLC.
- **Revenu payé par le passager.** `total_amount` comprend des taxes et des suppléments reversés (MTA, congestion, péage urbain) : ce n'est pas le revenu net du chauffeur ni de la compagnie.
- **Pourboires en espèces absents.** La TLC n'enregistre que les pourboires payés par carte. Le revenu d'un trajet payé en espèces est donc sous-estimé, et l'écart entre carte et espèces est en partie mécanique.
- **Zones inconnues exclues** (264 et 265). La zone retenue est celle de la prise en charge, pas celle de la dépose.
