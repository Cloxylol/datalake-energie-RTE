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

## La couche Silver

Bronze conserve trois formats bruts. Silver en fait un modèle commun, et
tout ce qu'elle fait est déclaré dans `conf/silver_mapping.yml` : le code
de `src/silver/` ne contient aucun nom de champ source en dur. Quand RTE
ajoute une filière ou renomme une colonne, on édite le YAML.

**Trois tables.** `grid_load` (consommation, prévisions RTE, intensité
carbone), `grid_generation` (une ligne par filière), `weather` (météo
horaire par ville + agrégat national pondéré). Toutes portent la clé
commune `(ts_utc, zone_id)` et les colonnes de traçabilité
`source / quality / ingested_at / source_file`.

**Validation de schéma, avec quarantaine.** Une ligne qui ne respecte pas
le contrat n'est jamais supprimée en silence : elle part dans
`/datalake/silver/_rejects/<table>/` avec son motif, sa colonne fautive et
sa charge utile complète. Quatre motifs : `null_key`, `cast_failed`,
`out_of_range`, `unexpected_value`. Au-delà de `max_reject_ratio` (25 %),
le job échoue plutôt que de publier une table amputée — un taux pareil
signale un mapping cassé, pas des données sales.

Hors bornes, deux traitements selon le mapping : `reject` sort la ligne
(une consommation de 200 GW invalide tout le reste), `null_out` neutralise
la seule mesure (une prévision aberrante ne doit pas faire perdre la
consommation mesurée de la même ligne). L'ordre d'appel n'est pas
négociable : on qualifie d'abord, on neutralise ensuite, sinon toute
mesure hors bornes ressemble à un cast en échec.

**Normalisation UTC.** `date_heure` porte un offset ISO explicite. Trois
cas : colonne déjà typée `timestamp` (Spark a lu l'offset, ne pas
reconvertir), texte avec offset, texte sans offset (interprété en
Europe/Paris). `ts_local` est conservée pour les features calendaires du
ML : la consommation suit le rythme humain, pas UTC.

**Déduplication par qualité.** Le même quart d'heure est publié en temps
réel, puis remplacé par du consolidé, puis par du définitif. La qualité
est lue **dans** la donnée (champ `nature`), pas déduite du chemin Bronze :
un lot mensuel peut être à cheval sur la frontière consolidé/définitif.

Sur `grid_load` la fusion se fait **mesure par mesure** et pas ligne par
ligne, parce que les deux flux sont complémentaires : le définitif ne
publie les mesures qu'au pas de 30 min alors que les prévisions sont au
quart d'heure. Garder bêtement la ligne consolidée perdrait la mesure
temps réel du même horodatage.

**Dépivotement des filières.** Une colonne par filière devient une ligne
par filière : le schéma absorbe une nouvelle filière sans changer. Chaque
ligne porte sa place dans la hiérarchie. Seuls les **agrégats** sont émis
par défaut, parce que `eolien` vaut déjà `eolien_terrestre +
eolien_offshore` : émettre les trois ferait double compte dès que Gold
somme la production totale. Les filières de détail sont déclarées dans le
mapping et s'activent avec `unpivot.include_levels`.

### Trois écarts que le mapping rend visibles

Relevés sur les API, pas sur une doc — c'est tout l'objet de
`scripts/explore_schema.py` :

| Constat | Conséquence |
|---|---|
| Temps réel : 41 champs. Consolidé : 37. `eolien_offshore` et `stockage_batterie` n'existent que d'un côté | Un champ absent est signalé puis mis à null, jamais fatal |
| `ech_comm_allemagne_belgique` arrive en entier d'un flux, en chaîne de l'autre | Typage explicite obligatoire ; un cast raté part en quarantaine au lieu de devenir un null muet |
| Open‑Meteo publie le vent en **km/h**, pas en m/s | Facteur de conversion déclaré dans le mapping, et l'unité annoncée par l'API (`hourly_units`) est vérifiée à chaque run. Une colonne `wind_speed_ms` qui contient des km/h ment de 3,6× et personne ne s'en aperçoit avant d'entraîner un modèle dessus |

Le fuseau d'Open‑Meteo est vérifié sur `utc_offset_seconds` et non sur le
libellé : l'API répond `GMT` quand on demande `UTC`.

Vérification sans cluster :

```bash
python scripts/test_silver_local.py
```

Le script fabrique un Bronze factice respectant l'arborescence réelle et
rejoue 33 contrôles : lecture des trois formats, conversion UTC dont la
nuit du changement d'heure, les quatre motifs de rejet, la fusion par
qualité, le dépivotement sans double compte, la conversion d'unités et
l'idempotence du rejeu.

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
conf/silver_mapping.yml       champs source → modèle commun, filières, bornes
src/common/config.py          chargement conf + construction des chemins (Layout)
src/common/hdfs_io.py         BronzeWriter : écriture atomique + _SUCCESS
src/ingestion/
  kafka_producer_eco2mix.py   polling REST → Kafka, clé = date_heure
  stream_bronze_eco2mix.py    Spark Structured Streaming → Bronze
  batch_eco2mix_cons.py       CSV mensuel → Bronze
  batch_open_meteo.py         JSON par ville × mois → Bronze
  batch_entsoe.py             XML mensuel → Bronze (optionnel)
src/silver/
  mapping.py                  lecture du mapping, résolution des noms de champs
  readers.py                  lecture Bronze : csv, json_envelope, json_arrays
  validation.py               contrôles de schéma + quarantaine _rejects
  transform.py                UTC, qualité, dédup, dépivotement, écriture
  silver_grid.py              eco2mix → grid_load + grid_generation
  silver_weather.py           Open-Meteo → weather
scripts/
  explore_schema.py           découverte des schémas réels
  test_idempotence.py         scénarios d'idempotence sans cluster
  test_silver_local.py        31 contrôles Silver sans HDFS ni cluster
```

## Reste à faire

- [x] `conf/silver_mapping.yml` — mapping champs source → modèle commun
- [x] `src/silver/` — validation de schéma, dédup, normalisation UTC, dépivotement des filières
- [ ] `src/gold/` — `mix_horaire`, `kpi_daily`, `ml_features`
- [ ] `dags/` — DAGs Airflow avec dépendances par Datasets
- [ ] `notebooks/` — restitution Pandas depuis Gold
- [ ] `src/ml/` — prévision de consommation à H+24, benchmarkée contre la
      prévision RTE présente dans le dataset

## Sources

- RTE / ODRÉ — <https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/>
- Open‑Meteo — <https://open-meteo.com/en/docs/historical-weather-api>
- ENTSO‑E Transparency — <https://transparency.entsoe.eu/>
