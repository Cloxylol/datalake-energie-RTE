# Datalake énergie — Bronze / Silver / Gold

Datalake sur HDFS ingérant trois sources hétérogènes autour du système
électrique français, orchestré par Airflow.

| Source | Format | Accès | Granularité | Couche |
|---|---|---|---|---|
| éCO2mix national temps réel (RTE) | JSON | polling REST → Kafka | 15 min | temps réel |
| éCO2mix consolidé 2012‑2024 (RTE) | CSV | export HTTP | 15 min | batch |
| Open‑Meteo archive, 5 villes | JSON | HTTP, sans clé | horaire | batch |
| ENTSO‑E prix day‑ahead *(optionnel)* | XML | HTTP + token | horaire | batch |

Trois formats, deux protocoles, deux granularités, quatre schémas distincts,
mais une clé de jointure commune `(ts_utc, zone_id)` qui rend la couche Gold
possible. C'est ce qui distingue un datalake de trois pipelines parallèles.

---

## Démarrage

```bash
cp .env.example .env          # optionnel : renseigner ENTSOE_TOKEN
docker compose up -d
docker compose ps             # attendre que namenode soit healthy
```

Créer l'arborescence HDFS :

```bash
docker compose exec namenode hdfs dfs -mkdir -p \
  /datalake/bronze /datalake/silver /datalake/gold /datalake/_checkpoints
docker compose exec namenode hdfs dfs -chmod -R 777 /datalake
```

Interfaces : HDFS `localhost:9870`, Spark `localhost:8081`,
Airflow `localhost:8080` (admin / admin).

## Avant d'écrire le job Silver

Les noms de champs éCO2mix évoluent (ajout de `stockage_batterie`,
`eolien_offshore`, découpage par technologie…). **Ne pas les figer depuis une
doc** : les lire sur l'API.

```bash
docker compose run --rm producer-eco2mix \
  python /opt/datalake/scripts/explore_schema.py --all \
  --out /opt/datalake/conf/schema_discovered.json
```

Le script classe les champs par catégorie et liste les filières de production
détectées, à reporter dans `conf/silver_mapping.yml`.

## Ingestion manuelle

```bash
# Batch éCO2mix — un lot par mois
docker compose run --rm producer-eco2mix \
  python /opt/datalake/src/ingestion/batch_eco2mix_cons.py \
  --start 2024-01-01 --end 2024-03-31

# Batch météo — 5 villes × N mois
docker compose run --rm producer-eco2mix \
  python /opt/datalake/src/ingestion/batch_open_meteo.py \
  --start 2024-01-01 --end 2024-03-31

# Streaming Kafka → Bronze
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/datalake/src/ingestion/stream_bronze_eco2mix.py --trigger availableNow
```

Vérifier :

```bash
docker compose exec namenode hdfs dfs -ls -R /datalake/bronze | head -30
docker compose exec namenode hdfs dfs -cat \
  /datalake/bronze/eco2mix_cons/year=2024/month=03/_SUCCESS
```

---

## Idempotence

L'exigence « un DAG interrompu doit pouvoir être relancé sans dupliquer les
données » est traitée différemment selon la couche.

**Bronze batch** — marker `_SUCCESS`, avec un ordre strict : on teste le
marker *avant* de télécharger, on écrit dans un `.part`, on renomme en
atomique, et on pose `_SUCCESS` *en dernier*. Un job tué entre l'écriture et
le commit ne laisse pas de marker : la relance retraite proprement le lot.
Le marker contient les métadonnées du lot (lignes, octets, format), ce qui
le rend auditable.

Un contrôle de cohérence précède le commit : un CSV de 12 octets ou un mois
à 400 lignes lève une exception plutôt que de produire un `_SUCCESS`
mensonger.

**Bronze streaming** — `checkpointLocation` pour les offsets Kafka et
`_spark_metadata` pour l'exactly‑once du sink fichier. Ce répertoire est
placé hors de `/datalake/bronze` : un `hdfs dfs -rm -r` de Bronze ne doit pas
détruire les offsets.

**Silver et Gold** — écrasement dynamique de partition :

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("year", "month").parquet(path)
```

Chaque job reçoit sa fenêtre en paramètre et n'écrase que les partitions
correspondantes. Sans `dynamic`, `overwrite` détruit **toute** la table :
c'est le piège classique.

Vérification sans cluster :

```bash
python scripts/test_idempotence.py
```

Le script rejoue cinq scénarios (double exécution, interruption avant commit,
relance après interruption, lot rejeté par le contrôle) sur un HDFS simulé
en mémoire.

## Partitionnement

Deux conventions coexistent, volontairement.

Le **batch** est partitionné sur la date métier (`year=/month=` des données),
parce que c'est ce qui rend le rejeu idempotent : relancer mars 2024 écrase
exactement la même partition.

Le **streaming** est partitionné sur la date d'ingestion
(`ingest_date=/ingest_hour=`), parce qu'au moment de l'écriture on ne connaît
pas la date métier de chaque message sans payer un shuffle qui tuerait la
latence. Silver rebascule tout sur la date métier.

## Bronze reste brut

Le job streaming ne parse pas le JSON : la valeur Kafka est stockée en
chaîne, accompagnée de colonnes techniques (`kafka_partition`,
`kafka_offset`, `bronze_ingested_at`) qui permettent de rejouer un intervalle
d'offsets précis. Les ingesteurs batch écrivent le CSV et le XML tels que
reçus, délimiteur d'origine compris. Toute la traçabilité vit dans `_SUCCESS`
ou dans les colonnes techniques, jamais dans le payload.

## Quotas

ODRÉ limite à 50 000 appels par utilisateur et par mois. Le producteur poll
toutes les 120 s, soit environ 21 600 appels par mois : confortable, mais
descendre l'intervalle sous 60 s ferait dépasser le quota. Le code gère le
429 avec back‑off.

## Structure

```
conf/sources.yml              sources, zones, chemins — aucune URL en dur ailleurs
src/common/config.py          chargement conf + construction des chemins (Layout)
src/common/hdfs_io.py         BronzeWriter : écriture atomique + _SUCCESS
src/ingestion/
  kafka_producer_eco2mix.py   polling REST → Kafka, clé = date_heure
  stream_bronze_eco2mix.py    Spark Structured Streaming → Bronze
  batch_eco2mix_cons.py       CSV mensuel → Bronze
  batch_open_meteo.py         JSON par ville × mois → Bronze
  batch_entsoe.py             XML mensuel → Bronze (optionnel)
scripts/
  explore_schema.py           découverte des schémas réels
  test_idempotence.py         scénarios d'idempotence sans cluster
```

## Reste à faire

- [ ] `conf/silver_mapping.yml` — mapping champs source → modèle commun
- [ ] `src/silver/` — validation de schéma, dédup, normalisation UTC, dépivotement des filières
- [ ] `src/gold/` — `mix_horaire`, `kpi_daily`, `ml_features`
- [ ] `dags/` — DAGs Airflow avec dépendances par Datasets
- [ ] `notebooks/` — restitution Pandas depuis Gold
- [ ] `src/ml/` — prévision de consommation à H+24, benchmarkée contre la
      prévision RTE présente dans le dataset

## Sources

- RTE / ODRÉ — <https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/>
- Open‑Meteo — <https://open-meteo.com/en/docs/historical-weather-api>
- ENTSO‑E Transparency — <https://transparency.entsoe.eu/>
