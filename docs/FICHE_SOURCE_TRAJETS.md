# Fiche source — Trajets des taxis jaunes de New York (TLC)

> Fiche générée le 05/10/2026 par `docs/generer_fiche_trajets.py` : tous les chiffres sont mesurés sur les fichiers avec DuckDB 1.5.6. Analyse détaillée : `yellow_tripdata_2025-01.parquet`.

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | Yellow Taxi Trip Records : un enregistrement par trajet de taxi jaune |
| Producteur des données | NYC Taxi and Limousine Commission (TLC), à partir des données transmises par les fournisseurs de systèmes d'enregistrement (TPEP) |
| Adresse (URL) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_AAAA-MM.parquet` ; page : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page ; dictionnaire : https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf |
| Accès (public, authentifié) | Public, sans authentification |
| Format du fichier | Apache Parquet (compression ZSTD, 4 groupes de lignes), un fichier par mois |
| Fréquence de publication | Mensuelle |
| Délai entre la période couverte et la publication | Environ deux mois (« typically with a two-month delay to allow time for full vendor submissions », page TLC) |

## Volume mesuré

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---|---|---|---|
| `yellow_tripdata_2025-01.parquet` | 59,2 Mo (59 158 238 octets) | 3 475 226 | 20 | DuckDB : `python docs/generer_fiche_trajets.py yellow_tripdata_2025-01.parquet` |

## Colonnes

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `VendorID` | INTEGER | Fournisseur du système d'enregistrement (TPEP) qui a transmis le trajet | 2 |
| `tpep_pickup_datetime` | TIMESTAMP | Date et heure de prise en charge (compteur enclenché), heure locale de New York | 2025-01-23 22:11:00 |
| `tpep_dropoff_datetime` | TIMESTAMP | Date et heure de dépose (compteur arrêté), heure locale de New York | 2025-01-19 00:00:00 |
| `passenger_count` | BIGINT | Nombre de passagers, saisi par le chauffeur | 1 |
| `trip_distance` | DOUBLE | Distance parcourue en miles, mesurée par le taximètre | 0 |
| `RatecodeID` | BIGINT | Tarif appliqué en fin de trajet | 1 |
| `store_and_fwd_flag` | VARCHAR | Trajet stocké dans le véhicule faute de connexion, puis transmis | N |
| `PULocationID` | INTEGER | Zone TLC de prise en charge (voir taxi_zone_lookup.csv) | 161 |
| `DOLocationID` | INTEGER | Zone TLC de dépose (voir taxi_zone_lookup.csv) | 236 |
| `payment_type` | BIGINT | Mode de paiement | 1 |
| `fare_amount` | DOUBLE | Prix de la course calculé par le taximètre (temps et distance), en dollars | 8,6 |
| `extra` | DOUBLE | Suppléments divers (heures de pointe, nuit), en dollars | 0 |
| `mta_tax` | DOUBLE | Taxe MTA déclenchée automatiquement selon le tarif, en dollars | 0,5 |
| `tip_amount` | DOUBLE | Pourboire, rempli automatiquement pour les cartes (espèces non incluses), en dollars | 0 |
| `tolls_amount` | DOUBLE | Total des péages payés pendant le trajet, en dollars | 0 |
| `improvement_surcharge` | DOUBLE | Supplément d'amélioration perçu à la prise en charge, en dollars | 1 |
| `total_amount` | DOUBLE | Montant total facturé au passager, hors pourboires en espèces, en dollars | 17,7 |
| `congestion_surcharge` | DOUBLE | Supplément de congestion de l'État de New York, en dollars | 2,5 |
| `Airport_fee` | DOUBLE | Frais de prise en charge aux aéroports LaGuardia et JFK, en dollars | 0 |
| `cbd_congestion_fee` | DOUBLE | Redevance de la zone de péage urbain de Manhattan (MTA), depuis le 5 janvier 2025, en dollars | 0,75 |

Exemple de valeur : la valeur la plus fréquente de la colonne.

## Codes

D'après le dictionnaire de données TLC (version du 18 mars 2025), avec la répartition mesurée dans `yellow_tripdata_2025-01.parquet`.

| Colonne | Valeur | Signification | Trajets |
|---|---|---|---|
| `VendorID` | 1 | Creative Mobile Technologies, LLC | 753 671 (21,69 %) |
| `VendorID` | 2 | Curb Mobility, LLC | 2 719 860 (78,26 %) |
| `VendorID` | 6 | Myle Technologies Inc | 489 (0,01 %) |
| `VendorID` | 7 | Helix | 1 206 (0,03 %) |
| `RatecodeID` | 1 | Tarif standard | 2 756 472 (79,32 %) |
| `RatecodeID` | 2 | JFK | 94 420 (2,72 %) |
| `RatecodeID` | 3 | Newark | 8 622 (0,25 %) |
| `RatecodeID` | 4 | Nassau ou Westchester | 7 092 (0,20 %) |
| `RatecodeID` | 5 | Tarif négocié | 26 501 (0,76 %) |
| `RatecodeID` | 6 | Trajet partagé | 7 (< 0,01 %) |
| `RatecodeID` | 99 | Non renseigné | 41 963 (1,21 %) |
| `RatecodeID` | (vide) | Non renseigné | 540 149 (15,54 %) |
| `store_and_fwd_flag` | N | Transmis directement | 2 927 431 (84,24 %) |
| `store_and_fwd_flag` | Y | Stocké dans le véhicule puis transmis plus tard | 7 646 (0,22 %) |
| `store_and_fwd_flag` | (vide) | Non renseigné | 540 149 (15,54 %) |
| `payment_type` | 0 | Flex Fare (tarif flexible) | 540 149 (15,54 %) |
| `payment_type` | 1 | Carte bancaire | 2 444 393 (70,34 %) |
| `payment_type` | 2 | Espèces | 390 429 (11,23 %) |
| `payment_type` | 3 | Gratuit | 23 773 (0,68 %) |
| `payment_type` | 4 | Litige | 76 481 (2,20 %) |
| `payment_type` | 5 | Inconnu | 1 (< 0,01 %) |
| `payment_type` | 6 | Trajet annulé | 0 |

## Ce qui a surpris

- **Dates hors période** : 22 trajets (< 0,01 %) ont une prise en charge en dehors de janvier 2025 ; les dates du fichier vont du 31/12/2024 au 01/02/2025.
- **Distances** : 90 893 trajets (2,62 %) ont une distance nulle, et 162 dépassent 100 miles.
- **Montants négatifs** : 63 037 trajets (1,81 %) ont un `total_amount` négatif (minimum : -901 $), probablement des annulations ou des corrections.
- **Durées** : 2 051 trajets se terminent avant ou au moment de leur début, et 1 375 durent plus de 3 heures.
- **Passagers** : 24 656 trajets déclarent 0 passager.
- **Valeurs vides** : `passenger_count` (540 149), `RatecodeID` (540 149), `store_and_fwd_flag` (540 149), `congestion_surcharge` (540 149), `Airport_fee` (540 149).
- **Noms de colonnes** : la casse varie (`VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, `Airport_fee`). D'où `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` au chargement.
