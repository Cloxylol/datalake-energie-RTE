# Schéma des tables Gold

Trois tables, produites par `src/gold/gold_build.py` à partir des tables
Silver `grid_load`, `grid_generation` et `weather`.

    spark-submit gold_build.py --start 2024-03-01 --end 2024-03-31

Le fenêtrage (les trois fenêtres, l'alignement sur le mois, ce que le job lit
par rapport à ce qu'il écrit) est décrit dans [`decisions.md`](decisions.md)
et n'est pas répété ici. Deux conséquences seulement, parce qu'elles se
lisent dans le schéma :

- **`window_start` / `window_end`** sont présentes dans les trois tables.
  Elles portent `window_written`, c'est-à-dire la fenêtre que le run a
  entièrement recalculée. Une partition dit ainsi elle-même quel run l'a
  produite, sans qu'on aille lire les logs.
- `mix_horaire` et `ml_features` sont partitionnées sur le mois **UTC**
  (`ts_utc`), `kpi_daily` sur le mois **local** (`date_local`). Ce ne sont
  pas les mêmes mois.

Conventions communes : les puissances sont en MW, les horodatages `ts_utc` en
UTC et `ts_local` en `Europe/Paris`, et une valeur absente est **null**,
jamais 0.

---

## `gold/mix_horaire`

Le pas horaire, une ligne par `(ts_utc, zone_id)`. C'est la table de
jointure : consommation RTE, production par filière et météo sur la même
clé. eco2mix est au quart d'heure et la météo à l'heure ; l'heure est le
plus petit dénominateur commun, les mesures infra-horaires y sont moyennées.

**Partitionnement** : `year` / `month`, dérivés de `ts_utc`.

### Clé et temps

| Colonne | Type | Description |
|---|---|---|
| `ts_utc` | timestamp | Début de l'heure, UTC. Clé avec `zone_id`. |
| `ts_local` | timestamp | Même instant en `Europe/Paris`. Sert au calendrier. |
| `zone_id` | string | Zone de jointure commune (`fr` aujourd'hui). |

### Consommation et prévisions

| Colonne | Type | Description |
|---|---|---|
| `consumption_mw` | double | Consommation moyenne de l'heure. |
| `forecast_j1_mw` | double | Prévision RTE J-1. Sert de benchmark au modèle. |
| `forecast_j_mw` | double | Prévision RTE du jour. |
| `forecast_error_mw` | double | `consumption_mw - forecast_j1_mw`. Signé : positif = RTE a sous-estimé. |
| `co2_rate_g_kwh` | double | Intensité carbone moyenne, gCO2/kWh. |
| `physical_exchange_mw` | double | Solde des échanges physiques aux frontières. **Négatif = export net.** Sans lui le bilan de l'heure ne boucle pas : la consommation n'égale pas la production nationale. |
| `n_points` | bigint | Nombre de points de l'heure où `consumption_mw` est **réellement renseignée** — c'est-à-dire sur combien de valeurs la moyenne de l'heure porte. Voir la note ci-dessous. Aucun filtrage n'est appliqué : c'est au lecteur de décider quoi faire d'une heure incomplète. |

> **`n_points` compte des valeurs, pas des lignes.** La distinction n'est pas
> théorique : sur le flux **définitif**, eco2mix publie une grille au quart
> d'heure mais ne renseigne la consommation qu'à `:00` et `:30`. Une heure y
> compte donc **4 lignes pour 2 valeurs**, et un compteur de lignes
> afficherait 4 en laissant croire que la moyenne porte sur quatre mesures.
> Mesuré sur mars 2024 : `n_points = 2` sur les 744 heures. `co2_rate_g_kwh`
> et `physical_exchange_mw` suivent exactement le même pas, `consumption_mw`
> sert de référence pour tout le groupe.
>
> La valeur attendue dépend donc du flux : **2** en définitif, et jusqu'à
> **4** sur le flux temps réel, interrogé toutes les 15 minutes. Ce qui se
> lit dans la colonne n'est pas tant sa valeur absolue que son écart à la
> valeur habituelle du mois : une heure en dessous est une heure trouée.

### Production par filière

Une colonne par filière, en MW, moyenne de l'heure.

**La liste vient de `conf/silver_mapping.yml`** (filières de niveau
`aggregate`, catégories `production` et `stockage`) et est passée
explicitement au pivot. Deux conséquences : ajouter une filière au mapping
ajoute une colonne ici sans toucher au code Gold, et le schéma **ne dépend
pas des données** — un mois sans solaire garde une colonne `solaire` pleine
de nulls au lieu de produire une partition aux colonnes différentes des
autres.

Colonnes au mapping v1, 12 filières :

| Catégorie | Colonnes |
|---|---|
| `production`, renouvelable | `eolien`, `solaire`, `hydraulique`, `bioenergies` |
| `production`, non renouvelable | `nucleaire`, `gaz`, `charbon`, `fioul`, `thermique` |
| `stockage` | `pompage`, `stockage_batterie`, `destockage_batterie` |

`thermique` est absente des flux nationaux actuels et reste donc nulle ;
elle est conservée pour les archives. Les filières de niveau `detail`
(`eolien_terrestre`, `gaz_tac`, `hydraulique_lacs`…) sont **exclues** : leurs
valeurs sont déjà comprises dans celles du parent, les émettre ferait double
compte.

### Agrégats de production

| Colonne | Type | Description |
|---|---|---|
| `generation_total_mw` | double | Somme des filières de catégorie **`production` uniquement**. |
| `generation_renewable_mw` | double | Somme des filières `production` marquées `renewable` dans le mapping. |
| `storage_net_mw` | double | Solde des filières de catégorie `stockage`, **signé** : négatif = le parc absorbe (pompage, charge batterie), positif = il restitue. |
| `renewable_share_pct` | double | `100 × generation_renewable_mw / generation_total_mw`, null si le total est nul ou négatif. |

> **Le stockage ne s'additionne pas à la production.** Le pompage et la
> charge batterie sont des filières négatives : les sommer avec la
> production revient à soustraire du parc ce qu'il produit, et écrase le
> dénominateur de `renewable_share_pct`. C'est `filiere_category`, portée
> par Silver, qui fait la séparation — pas une liste de noms recopiée dans
> Gold.

### Météo

Moyenne nationale (`zone_id = fr`), jointe sur l'heure.

| Colonne | Type | Description |
|---|---|---|
| `temperature_c` | double | Température, °C. |
| `humidity_pct` | double | Humidité relative, %. |
| `wind_speed_ms` | double | Vent, m/s. |
| `cloud_cover_pct` | double | Couverture nuageuse, %. |

### Qualité et partitions

| Colonne | Type | Description |
|---|---|---|
| `quality_rank` | int | **Pire** rang des lignes Silver qui ont formé l'heure. Échelle du mapping : `0` définitif, `1` consolidé, `2` temps réel, `9` inconnu — rang croissant = qualité décroissante. |
| `quality` | string | Libellé correspondant : `definitive`, `consolidated`, `realtime`, `unknown`. |
| `window_start`, `window_end` | date | `window_written` du run qui a produit la partition. |
| `year`, `month` | int | Partitions, dérivées de `ts_utc`. |

`quality_rank` couvre les deux flux **électriques** (consommation et
production) et **pas** la météo : il annonce la qualité de la mesure réseau,
pas celle de la température. Une heure moyennée sur du temps réel ne doit pas
se présenter comme définitive, d'où le pire rang et non le meilleur.

---

## `gold/kpi_daily`

Agrégats métier au pas journalier, une ligne par `(date_local, zone_id)`,
directement lisibles par le notebook de restitution.

**Partitionnement** : `year` / `month`, dérivés de **`date_local`** — donc
des mois **locaux**. La journée est celle vécue à Paris, pas la journée UTC.

| Colonne | Type | Description |
|---|---|---|
| `date_local` | date | Journée locale. Clé avec `zone_id`. |
| `zone_id` | string | Zone. Fait partie de la clé : sans elle le pic d'une journée serait celui de la zone la plus consommatrice et la jointure dupliquerait les lignes. |
| `consumption_gwh` | double | Énergie consommée sur la journée, GWh (somme des MW horaires / 1000). |
| `consumption_avg_mw` | double | Puissance moyenne de la journée. |
| `peak_mw` | double | Pointe de consommation de la journée. |
| `peak_hour_local` | int | Heure locale de la pointe, 0–23. |
| `co2_avg_g_kwh` | double | Intensité carbone moyenne de la journée. |
| `renewable_share_pct` | double | **Ratio des totaux** : `100 × Σ generation_renewable_mw / Σ generation_total_mw`. Les sommes sont au pas horaire, donc des MWh : c'est bien la part d'énergie de la journée. |
| `physical_exchange_avg_mw` | double | Solde moyen des échanges aux frontières. Négatif = exportateur net sur la journée. |
| `temperature_avg_c` | double | Température moyenne. |
| `temperature_min_c` | double | Température minimale. |
| `temperature_max_c` | double | Température maximale. |
| `hdd` | double | Degrés-jours de chauffe : `max(18 - temperature_avg_c, 0)`. |
| `cdd` | double | Degrés-jours de climatisation : `max(temperature_avg_c - 18, 0)`. |
| `forecast_mae_mw` | double | Erreur absolue moyenne de la prévision RTE J-1 sur la journée. |
| `n_hours` | bigint | Nombre d'heures présentes. **24 pour une journée complète**, 23 ou 25 aux changements d'heure, moins si la source a des trous. |
| `quality_rank` | int | Pire rang de qualité des heures de la journée. Même échelle que `mix_horaire`. |
| `window_start`, `window_end` | date | `window_written` du run. |
| `year`, `month` | int | Partitions, dérivées de `date_local`. |

> **`renewable_share_pct` est un ratio de sommes, pas une moyenne de
> pourcentages horaires.** Les deux ne coïncident que si la production est
> constante sur la journée. Dans une moyenne de pourcentages, une heure
> creuse très renouvelable pèse autant qu'une pointe fossile ; dans le mix
> réel elle pèse ses MWh.

---

## `gold/ml_features`

Table d'apprentissage : une ligne par `(ts_utc, zone_id)` disposant d'une
cible. Les lignes dont `target_consumption_h24` est nulle sont écartées.

**Partitionnement** : `year` / `month`, dérivés de `ts_utc`.

### Cible et antécédents

| Colonne | Type | Description |
|---|---|---|
| `target_consumption_h24` | double | **La cible** : consommation à H+24. |
| `consumption_mw` | double | Consommation de l'heure courante. |
| `lag_24h` | double | Consommation exactement 24 h avant. |
| `lag_48h` | double | Consommation exactement 48 h avant. |
| `lag_168h` | double | Consommation exactement 7 jours avant. |
| `roll_mean_24h` | double | Moyenne sur les 24 dernières heures révolues. |
| `roll_std_24h` | double | Écart-type sur la même fenêtre. |
| `rte_forecast_j1_mw` | double | Prévision J-1 de RTE. Benchmark : le modèle doit se comparer à ça. |

> **Les lags sont des décalages en temps, pas en lignes.** Le cadre de
> fenêtre est exprimé en secondes sur `ts_utc` : `lag_24h` est la valeur du
> point situé exactement 24 h avant, et **null** si ce point n'existe pas.
> Un `lag` compté en lignes serait allé chercher H-23 dans une série trouée
> et l'aurait appelé `lag_24h` — une erreur silencieuse qui contamine
> l'apprentissage. La liste des décalages est déclarée en un seul endroit
> (`FEATURES`, dans `gold_build.py`) : en ajouter un crée la colonne **et**
> allonge la fenêtre de lecture, sans constante à maintenir à côté.

### Calendrier

Calculé sur l'heure **locale** : la consommation suit le rythme humain, pas
UTC.

| Colonne | Type | Description |
|---|---|---|
| `hour` | int | Heure locale, 0–23. |
| `dow` | int | Jour de la semaine, 1 = dimanche … 7 = samedi (convention Spark). |
| `month_of_year` | int | Mois local, 1–12. |
| `is_weekend` | int | 1 si samedi ou dimanche. |
| `hour_sin`, `hour_cos` | double | Encodage cyclique de l'heure : 23 h et 0 h doivent être voisines. |
| `is_holiday` | int | 1 si jour férié français. Reste à 0 si l'API des jours fériés est injoignable — l'échec n'est pas bloquant, mais il est journalisé. |

### Météo et confort

| Colonne | Type | Description |
|---|---|---|
| `temperature_c` | double | Température de l'heure. |
| `hdd` | double | `max(18 - temperature_c, 0)`. |
| `cdd` | double | `max(temperature_c - 18, 0)`. |
| `wind_speed_ms` | double | Vent, m/s. Explique l'éolien. |
| `cloud_cover_pct` | double | Couverture nuageuse, %. Explique le solaire. |

### Contexte et partitions

| Colonne | Type | Description |
|---|---|---|
| `ts_utc`, `ts_local` | timestamp | Horodatages. |
| `zone_id` | string | Zone. C'est la clé de partitionnement des fenêtres de lag. |
| `n_points` | bigint | Repris de `mix_horaire` : sur combien de valeurs non nulles la consommation de l'heure a été moyennée. Permet d'exclure les heures trouées de l'apprentissage sans que Gold ait tranché à la place du modélisateur. |
| `window_start`, `window_end` | date | `window_written` du run. |
| `year`, `month` | int | Partitions, dérivées de `ts_utc`. |
