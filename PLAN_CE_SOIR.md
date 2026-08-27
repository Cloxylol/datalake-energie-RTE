# Plan pour ce soir

Rendu demain. Ordre strict, et **ne pas sauter d'étape**.

## Règle numéro un

Un mois de données. Pas 2012‑2024. Un pipeline complet sur mars 2024 vaut
infiniment mieux qu'un pipeline à moitié fait sur douze ans.

## Timing

### T+0 à 45 min — Faire monter la stack

```bash
cp .env.example .env
docker compose up -d namenode datanode kafka
docker compose ps          # attendre namenode healthy
```

Créer l'arborescence :

```bash
docker compose exec namenode hdfs dfs -mkdir -p \
  /datalake/bronze /datalake/silver /datalake/gold /datalake/_checkpoints
docker compose exec namenode hdfs dfs -chmod -R 777 /datalake
```

Vérifier <http://localhost:9870>. **Si HDFS ne monte pas en 45 minutes,
passer au plan B** (voir plus bas). Ne pas s'acharner.

### T+45 min — Vérifier les noms de champs

```bash
docker compose run --rm producer-eco2mix \
  python /opt/datalake/scripts/explore_schema.py --all
```

Le job Silver est tolérant (il détecte les colonnes présentes), mais le
**producteur Kafka** filtre sur `consommation IS NOT NULL`. Si ce champ
s'appelle autrement → HTTP 400. Corriger dans
`kafka_producer_eco2mix.py`, constante `LOAD_CANDIDATES` côté Silver.

### T+1 h — Ingérer un mois

```bash
docker compose run --rm producer-eco2mix \
  python /opt/datalake/src/ingestion/batch_eco2mix_cons.py \
  --start 2024-03-01 --end 2024-03-31

docker compose run --rm producer-eco2mix \
  python /opt/datalake/src/ingestion/batch_open_meteo.py \
  --start 2024-03-01 --end 2024-03-31

docker compose exec namenode hdfs dfs -ls -R /datalake/bronze | head -20
```

### T+1 h 30 — Le temps réel

```bash
docker compose up -d producer-eco2mix
docker compose logs -f producer-eco2mix     # vérifier que ça publie

docker compose up -d spark-master spark-worker
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/datalake/src/ingestion/stream_bronze_eco2mix.py --trigger availableNow
```

Le `--packages` télécharge des JARs au premier lancement. Prévoir du réseau
et 3–5 minutes.

### T+2 h 30 — Silver puis Gold

```bash
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  /opt/datalake/src/silver/silver_grid.py --start 2024-03-01 --end 2024-03-31

docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/datalake/src/silver/silver_weather.py --start 2024-03-01 --end 2024-03-31

docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  /opt/datalake/src/gold/gold_build.py --start 2024-03-01 --end 2024-03-31
```

**Le message à surveiller** dans les logs Gold :

```
mix_horaire : N ligne(s), dont M avec meteo jointe (X%)
```

Si X vaut 0, les deux sources ne couvrent pas la même période. C'est le seul
vrai point de rupture du pipeline.

### T+3 h 30 — Airflow

```bash
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

<http://localhost:8080>, admin/admin. Dépauser les DAGs, en déclencher un à
la main. **Il suffit qu'un DAG tourne vert pour valider l'exigence
d'orchestration.**

### T+4 h — Restitution et ML

```bash
docker compose run --rm producer-eco2mix \
  python /opt/datalake/src/ml/train_forecast.py --out /opt/datalake/models
jupyter notebook notebooks/insights.ipynb
```

## Plan B — si Docker résiste

Ne pas y laisser la nuit. Tout tourne en local avec Spark seul :

```bash
pip install pyspark pandas pyarrow scikit-learn matplotlib requests
```

Dans `conf/sources.yml`, remplacer `fs_uri` par `file:///tmp/datalake` et
`root` par `/tmp/datalake`. Les jobs Silver, Gold et ML sont identiques.
Tu perds HDFS et Airflow, mais tu as un livrable complet à montrer, et
tu expliques la stack Docker à l'oral.

**Ordre de sacrifice**, si le temps manque, du moins grave au plus grave :

1. ENTSO‑E (déjà abandonné, token trop lent)
2. Le ML (marqué bonus dans le sujet, non requis)
3. Le streaming Kafka en continu — se démontrer en `availableNow`
4. Airflow — le dernier à sacrifier, c'est une exigence explicite

## Ce qu'il faut montrer en soutenance

Trois choses, dans cet ordre :

```bash
python scripts/test_idempotence.py     # répond au "!!" du sujet
python scripts/test_silver_local.py    # 13/13, dont le changement d'heure
```

Puis le notebook, avec la corrélation consommation/température : elle
prouve que les deux sources hétérogènes se croisent vraiment.

**Le bug à raconter.** Le test du changement d'heure a révélé une double
conversion de fuseau : quand `inferSchema` avait déjà typé la colonne en
timestamp, un second `to_utc_timestamp` décalait toute la table d'une heure.
Corrigé en testant le *type* de la colonne, pas son contenu. Raconter ce
genre de chose vaut plus qu'un pipeline sans histoire.
