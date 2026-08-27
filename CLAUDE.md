# CLAUDE.md

## Contexte

Datalake énergie (TP Big Data) : ingestion de sources hétérogènes autour du
système électrique français, architecture **Bronze → Silver → Gold** sur HDFS,
orchestrée par Airflow. Voir `README.md` pour les sources et le démarrage.

Le projet est à deux. **Un collègue tient la couche Silver**
(`conf/silver_mapping.yml`, `src/silver/`). **Je tiens Gold et les DAGs**
(`src/gold/`, `dags/`). Ne pas proposer de refonte de Silver : signaler, et
laisser transmettre.

## Contrat de fenêtrage

`docs/decisions.md` fait foi pour tout ce qui touche à l'idempotence, au
fenêtrage et au partitionnement. **Le relire avant de modifier un job ou un
DAG.** L'invariant : *un job n'écrit jamais une partition qu'il n'a pas
entièrement recalculée.* Les trois fenêtres portent les mêmes noms dans le doc
et dans le code : `window_requested`, `window_read`, `window_written`.

## Périmètre : écriture interdite

Ne modifier sous aucun prétexte :

- `src/silver/`
- `src/ingestion/`
- `src/common/`
- `conf/`
- `scripts/explore_schema.py`
- `scripts/test_silver_local.py`

**Lecture et import autorisés** — et encouragés : `silver.mapping.load_mapping()`
est importable sans session Spark, c'est la source de vérité pour la liste des
filières et les noms de colonnes. Ne pas dupliquer ces listes dans Gold.

Si une correction paraît nécessaire dans ces fichiers : **la signaler et
s'arrêter.** Elle sera transmise au collègue.

## Git

Le dépôt porte en permanence une trentaine de fichiers modifiés non commités
qui ne m'appartiennent pas.

- **Jamais `git add -A`, jamais `git add .`, jamais `git commit -a`.**
- Chaque fichier est ajouté **nommément** : `git add docs/decisions.md`.
- Montrer `git status` avant chaque commit.
