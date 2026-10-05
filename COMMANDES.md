# Aide-mémoire des commandes

Toutes les commandes se lancent dans **WSL (Ubuntu)**, jamais dans PowerShell.
Docker Desktop doit être démarré côté Windows.

## 0. Ouvrir le projet

```bash
wsl ~                                   # depuis PowerShell : ouvre Ubuntu dans le dossier personnel
cd ~/nyc-taxi-pipeline                  # LE projet (jamais la copie sous /mnt/c)
source .venv/bin/activate               # environnement Python local
code .                                  # VS Code ; en bas à gauche : « WSL: Ubuntu »
```

## 1. Airflow (Astro CLI)

> ⚠ **Toutes les commandes `astro` se lancent depuis `airflow/`**, pas depuis la racine.
> Sinon : `Error: this is not an Astro project directory`.

```bash
cd ~/nyc-taxi-pipeline/airflow
```

### Démarrer, arrêter, reconstruire

```bash
astro dev start      # construit l'image si besoin et démarre les conteneurs
                     # interface : http://airflow.localhost:6563 (ou http://localhost:8080)
astro dev stop       # arrête les conteneurs ; l'historique des exécutions est conservé
astro dev restart    # arrête, RECONSTRUIT l'image et redémarre
astro dev ps         # liste les conteneurs du projet et leur état
astro dev kill       # ⚠ supprime conteneurs ET base Airflow (historique des runs, états)
                     #   dernier recours : au redémarrage, le catchup rejoue janvier à mars
```

### Quoi relancer après une modification

| J'ai modifié… | À faire |
|---|---|
| un DAG existant (`dags/*.py`) | rien : relu automatiquement en ~30 s |
| un **nouveau** fichier dans `dags/` | attendre jusqu'à 5 min, ou `astro dev run dags reserialize` |
| un fichier SQL de `include/` | rien : lu au moment de l'exécution de la tâche |
| `requirements.txt`, `packages.txt`, `Dockerfile` | `astro dev restart` (nouvelle image) |
| `airflow/.env` (connexion, clé) | `astro dev restart` (le `.env` n'est lu qu'au démarrage) |

### Commandes Airflow dans les conteneurs

```bash
astro dev run dags list                      # DAGs connus d'Airflow
astro dev run dags list-import-errors        # erreurs d'import ; attendu : « No data found »
astro dev run dags reserialize               # relit tout de suite le dossier dags/
astro dev run dags unpause nyc_taxi_pipeline # active le DAG (= interrupteur de l'interface)
astro dev run dags pause nyc_taxi_pipeline   # le met en pause
astro dev parse                              # vérifie que les DAGs s'importent sans erreur
astro dev bash                               # terminal dans le conteneur (exit pour sortir)
```

### Relancer un mois

- **Jamais le bouton Trigger** sur `nyc_taxi_pipeline` : il lance une exécution datée
  d'aujourd'hui, dont le fichier n'existe pas.
- Pour rejouer un mois : onglet **Runs** → l'exécution du mois → **Clear**.
- Rejouer un mois n'ajoute aucune ligne (PUT `SKIPPED`, COPY `0 files processed`).

### Journaux

```bash
# Journal du conteneur qui lit les DAGs (« Found N files », erreurs de lecture)
docker logs $(docker ps -qf name=dag-processor) --tail 50

# Ligne précise dans les logs des tâches
docker exec $(docker ps -qf name=scheduler) grep -rh "Bilan" /usr/local/airflow/logs/dag_id=nyc_taxi_pipeline
```

Dans l'interface : DAG → **Runs** → l'exécution → la tâche → **Journaux**.

### Recréer `airflow/.env` (nouvelle clé, nouveau poste)

```bash
cd ~/nyc-taxi-pipeline/airflow
PEM=$(awk 'NF {printf "%s\\n", $0}' ~/.ssh/snowflake/rsa_key.p8)
cat > .env <<EOF
AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI='{"conn_type":"snowflake","login":"AIRFLOW_SVC","extra":{"account":"ORGANISATION-COMPTE","warehouse":"NYC_TAXI_WH","database":"NYC_TAXI","role":"TRANSFORMER","private_key_content":"${PEM}"}}'
EOF
unset PEM
astro dev restart
```

Remplacer `ORGANISATION-COMPTE` par l'identifiant de compte. Ce fichier contient la
clé privée : il est ignoré par Git et par Docker, ne jamais le partager.

## 2. Snowflake

```bash
cd ~/nyc-taxi-pipeline
python snowflake/rendre.py --tous       # gabarits + .env → snowflake/build/*.sql
code snowflake/build/01_infrastructure.sql   # Ctrl+A, Ctrl+C → feuille Snowsight → Run All
python test/test_connexion.py           # attendu : ('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH')
```

- `03_verifications.sql` : à lancer **requête par requête** (Ctrl+Entrée), pas en Run All.
- Heures à la française dans Snowsight : `ALTER SESSION SET TIMEZONE = 'Europe/Paris';`

## 3. Chargement manuel (hors Airflow)

```bash
cd ~/nyc-taxi-pipeline
python ingestion/charger_mois.py 2025-01   # télécharge, PUT, COPY INTO ; rejouable
python ingestion/charger_mois.py --zones   # liste des 265 zones
```

## 4. Documentation

```bash
python docs/generer_fiche_trajets.py /tmp/tlc/yellow_tripdata_2025-01.parquet
# /tmp est vidé au redémarrage de WSL ; retélécharger si besoin :
mkdir -p /tmp/tlc && wget -P /tmp/tlc https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

## 5. Git

```bash
cd ~/nyc-taxi-pipeline
git status --short          # .env, airflow/.env, snowflake/build/ ne doivent JAMAIS apparaître
git add -A
git commit -m "message"
git push
```

## 6. En cas de souci

| Symptôme | Piste |
|---|---|
| `No module named …` | `.venv` pas activé, ou `pip install -r <dossier>/requirements.txt` |
| DAG absent de l'interface | `astro dev run dags list-import-errors`, puis vérifier que le fichier est bien dans `~/nyc-taxi-pipeline/airflow/dags/` |
| `conn_id snowflake_nyc_taxi isn't defined` | `airflow/.env` absent ou vide, puis `astro dev restart` |
| `JWT token is invalid` | la clé de `airflow/.env` ne correspond pas à celle de l'utilisateur de service |
| Docker ne répond pas | démarrer Docker Desktop ; vérifier Settings → Resources → WSL integration |
| Disque plein | `docker system df` puis `docker image prune` |
