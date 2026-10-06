# Réponse à la direction d'Hudson Cab Partners

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## Le périmètre

- **Données** : les trajets valides de `NYC_TAXI.MARTS.FCT_TRIPS`, soit 10 382 378 trajets, après nettoyage par le pipeline.
- **Période** : de janvier à mars 2025, soit 90 jours.
- **Zone** : la zone de prise en charge.
- **Heure** : l'heure locale de New York ; « 18 h » = de 18 h 00 à 18 h 59.
- **Exclusions** : les zones inconnues de la TLC (264 et 265).

## La requête

Elle est aussi dans `snowflake/04_verifications_marts.sql`, section 4.1.

```sql
WITH periode AS (
    SELECT COUNT(*) AS nb_jours FROM NYC_TAXI.MARTS.DIM_DATE
),
trajets AS (
    SELECT
        z.zone_name,
        z.borough,
        f.pickup_hour,
        f.total_amount,
        CASE p.payment_type_label
            WHEN 'Credit card'    THEN 'carte'
            WHEN 'Cash'           THEN 'especes'
            WHEN 'Flex Fare trip' THEN 'tarif_flexible'
            ELSE 'autres'                       -- gratuit, litige, inconnu, annulé
        END AS mode_paiement
    FROM NYC_TAXI.MARTS.FCT_TRIPS f
    JOIN NYC_TAXI.MARTS.DIM_ZONE z              ON z.zone_key = f.pickup_zone_key
    LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p ON p.payment_type_key = f.payment_type_key
    WHERE NOT z.is_unknown_zone
)
SELECT
    t.zone_name                                                          AS zone,
    t.borough                                                            AS arrondissement,
    t.pickup_hour                                                        AS heure,
    COUNT(*)                                                             AS nb_trajets,
    ROUND(COUNT(*) / ANY_VALUE(p.nb_jours), 1)                           AS trajets_par_jour,
    ROUND(AVG(t.total_amount), 2)                                        AS revenu_moyen_usd,
    -- part des trajets par mode de paiement (%)
    ROUND(COUNT_IF(t.mode_paiement = 'carte')          * 100 / COUNT(*), 1) AS pct_carte,
    ROUND(COUNT_IF(t.mode_paiement = 'especes')        * 100 / COUNT(*), 1) AS pct_especes,
    ROUND(COUNT_IF(t.mode_paiement = 'tarif_flexible') * 100 / COUNT(*), 1) AS pct_tarif_flexible,
    ROUND(COUNT_IF(t.mode_paiement = 'autres')         * 100 / COUNT(*), 1) AS pct_autres,
    -- revenu moyen d'un trajet par mode de paiement ($)
    ROUND(AVG(IFF(t.mode_paiement = 'carte',          t.total_amount, NULL)), 2) AS revenu_carte_usd,
    ROUND(AVG(IFF(t.mode_paiement = 'especes',        t.total_amount, NULL)), 2) AS revenu_especes_usd,
    ROUND(AVG(IFF(t.mode_paiement = 'tarif_flexible', t.total_amount, NULL)), 2) AS revenu_tarif_flexible_usd,
    ROUND(AVG(IFF(t.mode_paiement = 'autres',         t.total_amount, NULL)), 2) AS revenu_autres_usd
FROM trajets t
CROSS JOIN periode p
GROUP BY 1, 2, 3
ORDER BY nb_trajets DESC
LIMIT 10;
```

| Colonne | Ce qu'elle mesure |
|---|---|
| `nb_trajets`, `trajets_par_jour` | trajets partis de la zone à cette heure sur les 90 jours, puis par jour en moyenne |
| `revenu_moyen_usd` | montant moyen payé par le passager (`total_amount`), tous modes de paiement confondus |
| `pct_carte`, `pct_especes`, `pct_tarif_flexible`, `pct_autres` | part des trajets de chaque mode de paiement (total : 100 %) |
| `revenu_carte_usd` … `revenu_autres_usd` | revenu moyen d'un trajet pour chaque mode de paiement |

Le mode de paiement vient de la dimension `DIM_PAYMENT_TYPE`. « Tarif flexible » (code 0) est un prix fixé à l'avance ; « Autres » regroupe les courses gratuites, en litige, de mode inconnu ou annulées.

## Le résultat : les 10 premiers créneaux zone × heure

Tous à Manhattan. Classement par nombre de trajets.

### La demande et le revenu moyen

| Rang | Zone | Heure | Trajets | Trajets par jour | Revenu moyen ($) |
|---|---|---|---|---|---|
| 1 | Midtown Center | 18 h | 45 978 | 510,9 | 24,40 |
| 2 | Midtown Center | 17 h | 45 063 | 500,7 | 28,57 * |
| 3 | Midtown Center | 19 h | 38 770 | 430,8 | 23,62 |
| 4 | Midtown Center | 20 h | 38 518 | 428,0 | 22,67 |
| 5 | Midtown Center | 16 h | 36 702 | 407,8 | 25,77 |
| 6 | Upper East Side North | 15 h | 36 598 | 406,6 | 20,37 |
| 7 | Upper East Side South | 14 h | 36 482 | 405,4 | 19,98 |
| 8 | Upper East Side South | 15 h | 36 399 | 404,4 | 19,98 |
| 9 | Times Sq/Theatre District | 21 h | 36 340 | 403,8 | 23,58 |
| 10 | Upper East Side South | 18 h | 36 093 | 401,0 | 21,23 |

\* environ 25,6 $ sans une course aberrante à 132 555 $ : voir « Les limites ».

### Le détail par mode de paiement

| Rang | Carte (%) | Espèces (%) | Tarif flexible (%) | Autres (%) | Carte ($) | Espèces ($) | Tarif flexible ($) | Autres ($) |
|---|---|---|---|---|---|---|---|---|
| 1 | 83,2 | 8,8 | 6,4 | 1,5 | 25,06 | 19,93 | 22,80 | 21,37 |
| 2 | 84,4 | 9,5 | 4,6 | 1,5 | 26,35 | 20,74 | 24,16 | 213,41 * |
| 3 | 81,7 | 8,0 | 8,8 | 1,5 | 24,25 | 19,36 | 22,39 | 19,31 |
| 4 | 72,3 | 6,8 | 19,7 | 1,2 | 23,61 | 18,61 | 20,84 | 18,75 |
| 5 | 83,6 | 11,5 | 3,1 | 1,7 | 26,53 | 21,27 | 24,41 | 21,69 |
| 6 | 81,3 | 11,0 | 6,7 | 0,9 | 20,64 | 17,12 | 22,85 | 17,40 |
| 7 | 81,8 | 12,8 | 4,2 | 1,2 | 20,41 | 16,85 | 22,10 | 16,35 |
| 8 | 82,4 | 12,3 | 4,2 | 1,2 | 20,34 | 16,94 | 22,90 | 16,72 |
| 9 | 71,2 | 10,2 | 16,8 | 1,9 | 24,50 | 18,94 | 22,96 | 19,18 |
| 10 | 81,9 | 9,5 | 7,4 | 1,3 | 21,54 | 17,97 | 22,56 | 17,76 |

\* une seule course « No charge » à 132 555,41 $ porte cette moyenne : voir « Les limites ».

## Ce qu'il faut en retenir

1. **Où et quand** : la demande se concentre à Manhattan, où se trouvent les dix créneaux les plus chargés. Midtown Center en occupe cinq, de 16 h à 21 h, avec un pic à 18 h : 511 courses par jour en moyenne. L'Upper East Side domine l'après-midi (14 h à 16 h), et Times Square le soir à 21 h.
2. **Combien rapporte un trajet** : de 20 à 26 $ en moyenne dans ces créneaux. Les courses partant de Midtown Center en fin d'après-midi rapportent le plus (24 à 26 $), celles de l'Upper East Side le moins (environ 20 $).
3. **Mode de paiement** : la carte domine (71 à 84 % des courses) et rapporte 3,40 à 5,60 $ de plus que les espèces (7 à 13 % des courses), en partie parce que seuls les pourboires par carte sont enregistrés. Le tarif flexible, un prix fixé à l'avance, monte à 17-20 % des courses le soir (20 h-21 h), pour 21 à 24 $ par course : plus que les espèces partout, et même plus que la carte dans l'Upper East Side.

## Les limites

- **Trois mois d'hiver.** La demande change avec les saisons (tourisme d'été, fêtes de fin d'année). Le classement est à confirmer sur une année complète.
- **Trajets écartés.** 815 648 lignes de RAW sur 11 198 026, soit 7,3 %, ne sont pas dans `FCT_TRIPS`. Elles ont toutes été écartées par les règles de nettoyage ; aucun doublon n'a été trouvé sur la période. Le détail :
  - 489 135 montants nuls ou négatifs ;
  - 292 680 distances nulles ou supérieures à 100 miles ;
  - 29 495 durées nulles ou négatives ;
  - 4 252 durées de plus de 3 heures ;
  - 86 prises en charge hors du mois du fichier.

  Selon le mois, la part écartée va de 6,4 % (janvier) à 7,7 % (mars). Le détail par fichier est dans `MART_DATA_QUALITY`.
- **Une valeur aberrante non filtrée.** Les règles écartent les montants négatifs, pas les montants démesurés. Une course « No charge » du 21 février 2025 à 17 h 28, depuis Midtown Center, affiche 132 555,41 $ pour 2,2 miles. À elle seule, elle porte le revenu moyen de son créneau à 28,57 $ au lieu d'environ 25,6 $. Une règle de plus, par exemple un montant maximal, serait à proposer à l'analyste.
- **Toute la ville, pas seulement la flotte d'Hudson Cab.** Les fichiers de la TLC couvrent tous les taxis jaunes de New York, sans dire à quelle compagnie appartient chaque taxi. Le résultat décrit le marché, pas l'activité des 180 taxis.
- **Demande servie seulement.** On ne voit que les trajets réalisés. Les clients qui n'ont pas trouvé de taxi n'apparaissent pas, ni les VTC (Uber, Lyft), qui sont dans d'autres fichiers TLC.
- **Classement par volume, pas par revenu.** Le top 10 retient les créneaux les plus fréquentés. Les zones où une course rapporte le plus, comme les aéroports, n'y figurent pas : voir la requête 4.3 de `snowflake/04_verifications_marts.sql`.
- **Semaine et week-end confondus.** La demande d'un créneau peut différer fortement entre les deux ; `MART_ZONE_HOURLY_DEMAND` les sépare (`is_weekend`).
- **Revenu payé par le passager.** `total_amount` comprend des taxes et des suppléments reversés (MTA, congestion, péage urbain) : ce n'est pas le revenu net du chauffeur ni de la compagnie.
- **Pourboires en espèces absents.** La TLC n'enregistre que les pourboires payés par carte. Le revenu d'un trajet payé en espèces est donc sous-estimé, et l'écart entre carte et espèces est en partie mécanique.
- **Zones inconnues exclues** (264 et 265). La zone retenue est celle de la prise en charge, pas celle de la dépose.
