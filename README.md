# Pipeline médaillon NYC Yellow Taxi : Snowflake et Airflow

Hudson Cab Partners exploite 180 taxis jaunes à New York. Jusqu'ici, son analyste téléchargeait chaque mois le fichier de la Taxi and Limousine Commission (TLC) et le retraitait à la main dans un notebook. Ce dépôt remplace ce travail par une chaîne **automatisée, rejouable et contrôlée** :

- un entrepôt **Snowflake** à quatre couches, avec un rôle aux droits minimaux et un utilisateur de service authentifié par paire de clés ;
- un pipeline **Airflow 3** (Astro CLI) qui, chaque mois, charge le fichier TLC, exécute les règles SQL de l'analyste dans le bon ordre et contrôle la qualité des données.

Question à laquelle il répond : **où et quand la demande de taxis jaunes est-elle la plus forte, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?** La réponse est dans [`docs/REPONSE.md`](docs/REPONSE.md).

Périmètre : janvier, février et mars 2025, soit 11 198 026 trajets bruts et 10 382 378 trajets valides.

## Architecture

![Schéma du pipeline](docs/architecture.png)

| Couche (schéma Snowflake) | Contenu | Produit par |
|---|---|---|
| `RAW` | Copie fidèle des fichiers : `YELLOW_TRIPDATA`, `TAXI_ZONE_LOOKUP`, plus deux colonnes techniques (`_source_file`, `_loaded_at`). Contient aussi le stage et les formats de fichier. | `PUT` + `COPY INTO` (script Python ou DAG) |
| `STAGING` | Vues de renommage et de typage, sans filtre ; tables de codes (paiement, tarif, fournisseur) | `airflow/include/sql/staging/` |
| `INTERMEDIATE` | Chaque trajet reçoit sa raison de rejet éventuelle ; puis les trajets valides, dédoublonnés et enrichis (durée, vitesse, taux de pourboire) | `airflow/include/sql/intermediate/` |
| `MARTS` | 5 dimensions, la table de faits `FCT_TRIPS`, 3 tables d'analyse (revenu quotidien, demande par zone et par heure, qualité) | `airflow/include/sql/marts/` |

| Outil | Rôle |
|---|---|
| Snowflake | Stocke et calcule. Un seul warehouse XS, suspendu après 60 s d'inactivité. |
| Python (`ingestion/charger_mois.py`) | Charge un mois à la demande, hors Airflow : téléchargement, `PUT`, `COPY INTO`. |
| Airflow (`airflow/`) | Orchestre chaque mois le chargement, les 16 fichiers SQL fournis et les contrôles, dans l'ordre. |
| Docker et Astro CLI | Font tourner Airflow en local. |
| GitHub | Versionne le code, sans aucun secret. |

## Contenu du dépôt

```
.
├── README.md
├── COMMANDES.md                  aide-mémoire des commandes (Airflow, Snowflake, Git)
├── CONTRAT_RAW.md                noms imposés par les fichiers SQL fournis
├── ETAPES.md                     déroulé du brief, journée par journée
├── .env.example                  configuration du script Python et des scripts SQL (copier en .env)
├── snowflake/
│   ├── 01_infrastructure.sql     rôle, warehouse, base, schémas, droits, utilisateur de service, plafond
│   ├── 02_raw.sql                formats de fichier, stage, tables RAW
│   ├── 03_verifications.sql      droits, accès refusés, contrat RAW, historique de chargement, crédits
│   ├── 04_verifications_marts.sql  seuil des contrôles, rejouabilité, anomalies, réponse à la direction
│   ├── rendre.py                 remplace les variables {{ ... }} des scripts par les valeurs du .env
│   └── requirements.txt
├── ingestion/
│   ├── charger_mois.py           charge un mois (ou les zones) dans RAW
│   └── requirements.txt
├── test/test_connexion.py        vérifie la connexion par paire de clés
├── airflow/                      projet Astro
│   ├── dags/nyc_taxi_pipeline.py   chargement, transformations, contrôles
│   ├── dags/test_connexion_snowflake.py
│   ├── include/sql/              fichiers SQL fournis (non modifiés) + controles/
│   ├── requirements.txt
│   └── .env.example              format de la connexion Snowflake (copier en .env)
└── docs/
    ├── architecture.png
    ├── FICHE_SOURCE_TRAJETS.md   fiche source des trajets (chiffres mesurés)
    ├── generer_fiche_trajets.py  mesure le fichier Parquet avec DuckDB
    ├── REPONSE.md                réponse à la direction
    └── captures/                 preuves pour la démonstration
```

## Prérequis

- Python 3.10 ou plus récent, Git, OpenSSL ;
- Docker, et [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) ;
- un compte Snowflake (essai gratuit, édition Enterprise, région européenne), avec un utilisateur qui peut prendre les rôles USERADMIN, SYSADMIN et ACCOUNTADMIN ;
- sous Windows : tout se fait dans **WSL (Ubuntu)**, avec le projet dans le dossier personnel (`~`), pas sous `/mnt/c`.

`bash verifier_poste.sh` doit afficher `OK` partout.

## Installation pas à pas

### 1. Récupérer le projet

```bash
git clone https://github.com/aflo59/nyc-taxi-pipeline.git ~/nyc-taxi-pipeline
cd ~/nyc-taxi-pipeline
python3 -m venv .venv && source .venv/bin/activate
pip install -r snowflake/requirements.txt -r ingestion/requirements.txt -r docs/requirements.txt
```

### 2. Générer la paire de clés de l'utilisateur de service

```bash
mkdir -p ~/.ssh/snowflake && cd ~/.ssh/snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
chmod 600 rsa_key.p8
cd ~/nyc-taxi-pipeline
```

La clé privée (`rsa_key.p8`) reste sur le poste : elle ne va ni dans Git, ni dans une messagerie, ni dans l'image Docker.

### 3. Configurer le projet

```bash
cp .env.example .env
```

Renseigner `SNOWFLAKE_ACCOUNT` (format `ORGANISATION-COMPTE`, obtenu dans Snowsight avec `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`). Les autres valeurs ont des valeurs par défaut : rôle `TRANSFORMER`, warehouse `NYC_TAXI_WH`, utilisateur `AIRFLOW_SVC`, plafond de 10 crédits par mois.

### 4. Créer l'entrepôt Snowflake

```bash
python snowflake/rendre.py --tous      # gabarits + .env → snowflake/build/*.sql (ignoré par Git)
```

Dans Snowsight, avec un utilisateur administrateur :

1. `snowflake/build/01_infrastructure.sql` en entier (**Run All**) ;
2. `snowflake/build/02_raw.sql` en entier.

Vérifier la connexion de l'utilisateur de service :

```bash
python test/test_connexion.py          # attendu : ('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH')
```

### 5. Charger un mois à la main (facultatif)

```bash
python ingestion/charger_mois.py --zones     # les 265 zones (nécessaire une fois)
python ingestion/charger_mois.py 2025-01     # 3 475 226 trajets ; relancer n'ajoute rien
```

### 6. Lancer Airflow

```bash
cd airflow
cp .env.example .env                   # puis y coller la clé privée sur une ligne (voir le fichier)
astro dev start                        # interface : adresse affichée à la fin de la commande
```

Dans l'interface, activer le DAG `nyc_taxi_pipeline` avec son interrupteur, **sans** cliquer sur Trigger. Airflow crée et exécute une exécution par mois : janvier, février, mars. Les zones doivent avoir été chargées (étape 5) : le DAG ne charge que les trajets.

### 7. Vérifier

```bash
python snowflake/rendre.py --tous
```

Puis, dans Snowsight, requête par requête : `snowflake/build/03_verifications.sql` et `snowflake/build/04_verifications_marts.sql`.

## Le DAG `nyc_taxi_pipeline`

Une exécution traite **un mois** : celui de sa date logique, jamais celui du jour. Avec `catchup=True`, activer le DAG rejoue janvier, février et mars, un mois à la fois (`max_active_runs=1`).

```
verifier_disponibilite → telecharger_et_deposer → copier_dans_la_table
  → controle_raw_mois_charge
  → 00_tables + staging (codes_tlc, stg_tlc__taxi_zones, stg_tlc__yellow_trips)
  → intermediate : int_trips__flagged → controle_trajets_ecartes → int_trips__enriched
  → marts : dim_date, dim_payment_type, dim_rate_code, dim_vendor, dim_zone, mart_data_quality,
            fct_trips → controle_trajets_en_double → mart_daily_revenue, mart_zone_hourly_demand
```

**Une tâche par fichier SQL**, nommée comme lui, et **regroupée par couche** (`TaskGroup`). L'ordre se déduit des fichiers : chacun s'exécute après ceux qui créent les tables qu'il lit. Par exemple, `int_trips__flagged` lit la vue `STG_TLC__YELLOW_TRIPS` et écrit dans une table créée par `00_tables`.

Airflow remplace les variables des fichiers avant de les exécuter. `{{ ds }}` donne le premier jour du mois traité, et `{{ params.xxx }}` un paramètre du DAG : seuils de distance et de durée, bornes du calendrier, seuil du contrôle. Les fichiers qui contiennent plusieurs instructions (`DELETE` puis `INSERT`) sont découpés avec `split_statements=True`.

### Les contrôles

Un contrôle est une requête qui renvoie une ligne. Si une valeur est fausse, la tâche échoue, et toutes les tâches qui en dépendent passent en `upstream_failed` au lieu de s'exécuter. Les contrôles n'ont pas de relance automatique (`retries=0`) : relancer ne change pas les données.

| Contrôle | Placé après | Vérifie | Pourquoi là |
|---|---|---|---|
| `raw_mois_charge` (fourni) | le `COPY INTO` | le mois est présent dans RAW | inutile de transformer un mois vide |
| `trajets_ecartes` | `int_trips__flagged` | la part de trajets écartés reste sous `max_pct_trajets_ecartes` (10 %) | un fichier anormal n'atteint pas la table de faits |
| `trajets_en_double` | `fct_trips` | chaque `trip_sk` est unique dans le mois | les tables d'analyse ne comptent pas deux fois un trajet |

### Rejouable

Relancer un mois (**Clear** sur son exécution) ne change aucun compte de lignes :

- `PUT ... OVERWRITE=FALSE` ne renvoie pas un fichier déjà présent ;
- `COPY INTO` ignore un fichier déjà chargé ;
- les tables alimentées mois par mois effacent le mois avant de le réinsérer ;
- les autres tables sont recréées (`CREATE OR REPLACE`).

La preuve est dans `04_verifications_marts.sql`, section 2.

### Relances automatiques

Les tâches de chargement et de transformation ont 2 relances, à 5 minutes d'intervalle. Elles couvrent une coupure réseau, un fichier pas encore publié ou un Snowflake momentanément indisponible.

## Choix techniques

- **Un stage interne.** `PUT` dépose le fichier dans Snowflake, puis `COPY INTO` le charge en parallèle. Snowflake garde la mémoire des fichiers chargés, ce qui rend le chargement rejouable sans code supplémentaire. Le fichier est déposé à la racine du stage : `_source_file` contient exactement son nom, dont les fichiers SQL extraient le mois.
- **Un warehouse XS, suspendu après 60 s.** C'est la plus petite taille, soit 1 crédit par heure d'activité, et elle suffit pour 3,5 millions de lignes par mois. Un resource monitor coupe le warehouse à 100 % du quota mensuel.
- **Des types numériques à décimales en RAW.** Les montants et les distances sont en `NUMBER(10,2)`, qui garde les centimes là où un type entier les arrondirait. Une colonne entière un mois et décimale le suivant est acceptée. `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` absorbe les changements de casse des noms de colonnes (`Airport_fee`). Les colonnes de données n'ont pas de `NOT NULL` : les fichiers réels contiennent des vides, et les anomalies se traitent en INTERMEDIATE.
- **La date logique.** Le fichier se déduit du mois traité par l'exécution, pas de la date du jour. C'est ce qui permet de rejouer un ancien mois, et de rattraper l'historique avec `catchup`.
- **Des droits minimaux.** Le rôle `TRANSFORMER` a `USAGE` sur le warehouse (ni `OPERATE`, ni `MODIFY`). Il peut créer des tables, un stage et des formats dans RAW, et des tables et des vues dans les trois autres schémas. Il n'a ni `CREATE SCHEMA`, ni `CREATE DATABASE`, ni accès à la facturation. Aucun outil n'utilise ACCOUNTADMIN.
- **Aucun secret dans Git ni dans l'image.** L'authentification se fait par paire de clés. La clé privée est lue depuis le poste (script Python) ou depuis `airflow/.env` (Airflow). Ces deux fichiers sont ignorés par Git et par Docker.
- **Des scripts SQL paramétrés.** Les noms libres (rôle, warehouse, utilisateur, plafond) viennent du `.env` grâce à `rendre.py`. Les noms imposés par le contrat restent écrits en dur.

## Résultats

| Table | Lignes attendues | Lignes obtenues |
|---|---|---|
| `RAW.YELLOW_TRIPDATA` | 11 198 026 | |
| `RAW.TAXI_ZONE_LOOKUP` | 265 | |
| `INTERMEDIATE.INT_TRIPS__FLAGGED` | 11 198 026 | |
| `MARTS.FCT_TRIPS` | 10 382 378 | |
| `MARTS.MART_ZONE_HOURLY_DEMAND` | 11 524 | |
| `MARTS.MART_DATA_QUALITY` | 18 | |

Crédits consommés pendant le projet : _à compléter_ (`03_verifications.sql`, section 4).

## Captures

| Capture | Fichier |
|---|---|
| Droits du rôle des outils | `docs/captures/01_droits_role.png` |
| Accès refusé hors périmètre | `docs/captures/02_acces_refuse.png` |
| Historique de chargement Snowflake (`COPY_HISTORY`) | `docs/captures/03_historique_chargement.png` |
| Les trois exécutions réussies | `docs/captures/04_executions_airflow.png` |
| Le graphe du DAG | `docs/captures/05_graphe_dag.png` |
| Un contrôle en échec, la suite non exécutée | `docs/captures/06_controle_en_echec.png` |
| Relance de février sans doublon | `docs/captures/07_relance_sans_doublon.png` |
| Suivi des crédits | `docs/captures/08_credits.png` |

## Auteurs

- Florian
