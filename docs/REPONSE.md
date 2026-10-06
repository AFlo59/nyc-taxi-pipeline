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

| Rang | Zone | Arrondissement | Heure | Trajets | Trajets par jour | Revenu moyen ($) | Carte ($) | Espèces ($) | % carte |
|---|---|---|---|---|---|---|---|---|---|
| 1 | | | | | | | | | |
| 2 | | | | | | | | | |
| 3 | | | | | | | | | |
| 4 | | | | | | | | | |
| 5 | | | | | | | | | |
| 6 | | | | | | | | | |
| 7 | | | | | | | | | |
| 8 | | | | | | | | | |
| 9 | | | | | | | | | |
| 10 | | | | | | | | | |

> À remplir avec le résultat de la requête dans Snowsight, en recopiant les valeurs ou en téléchargeant le résultat.

## Ce qu'il faut en retenir

Trois phrases, pour quelqu'un qui ne lit pas le SQL :

1. **Où et quand** : …
2. **Combien rapporte un trajet** : …
3. **Mode de paiement** : …

## Les limites

- **Trois mois d'hiver.** La demande change avec les saisons (tourisme d'été, fêtes de fin d'année). Le classement est à confirmer sur une année complète.
- **Trajets écartés.** 815 648 lignes de RAW sur 11 198 026, soit 7,3 %, ne sont pas dans `FCT_TRIPS`. Ce sont des trajets écartés par les règles de nettoyage (distance nulle, montant négatif, durée impossible) ou des doublons. Le détail par raison est dans `MART_DATA_QUALITY`.
- **Toute la ville, pas seulement la flotte d'Hudson Cab.** Les fichiers de la TLC couvrent tous les taxis jaunes de New York, sans dire à quelle compagnie appartient chaque taxi. Le résultat décrit le marché, pas l'activité des 180 taxis.
- **Demande servie seulement.** On ne voit que les trajets réalisés. Les clients qui n'ont pas trouvé de taxi n'apparaissent pas, ni les VTC (Uber, Lyft), qui sont dans d'autres fichiers TLC.
- **Revenu payé par le passager.** `total_amount` comprend des taxes et des suppléments reversés (MTA, congestion, péage urbain) : ce n'est pas le revenu net du chauffeur ni de la compagnie.
- **Pourboires en espèces absents.** La TLC n'enregistre que les pourboires payés par carte. Le revenu d'un trajet payé en espèces est donc sous-estimé, et l'écart entre carte et espèces est en partie mécanique.
- **Zones inconnues exclues** (264 et 265). La zone retenue est celle de la prise en charge, pas celle de la dépose.
