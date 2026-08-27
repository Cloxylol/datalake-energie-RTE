# Décisions d'architecture

## Fenêtrage et idempotence

**Invariant : un job n'écrit jamais une partition qu'il n'a pas entièrement
recalculée.**

L'écrasement dynamique de partition remplace la partition *entière* par ce que
le DataFrame contient. Écrire un sous-ensemble d'une partition n'est donc pas
une mise à jour partielle : c'est une suppression du reste. La fenêtre traitée
doit être alignée sur l'unité de partitionnement de **la table cible** — ce
n'est pas « le mois » en général, c'est l'unité de chaque table.

Trois fenêtres, nommées pareil dans le code et ici :

- `window_requested` — ce que l'appelant demande (DAG ou ligne de commande).
- `window_read` — ce qu'il faut lire pour calculer juste. Dérivée des features,
  jamais constante : 8 jours en amont parce que `lag_168h` remonte à 7 jours et
  qu'il faut une marge d'alignement horaire, 1 jour en aval parce que
  `lead(24)` construit `target_consumption_h24`. Ajouter un lag plus long
  allonge `window_read` mécaniquement, sans qu'on touche à une constante.
- `window_written` — `window_requested` aligné sur l'unité de partition de la
  table cible.

Le job réalise l'alignement lui-même plutôt que de faire confiance à son
appelant : un `spark-submit` manuel doit être aussi sûr qu'un déclenchement
Airflow. Et il ne se contente pas de le journaliser — `window_written` est
**matérialisée en sortie**, sur le modèle des métadonnées portées par les
markers `_SUCCESS` de Bronze : une partition doit pouvoir dire quelle fenêtre
l'a produite, sans qu'on aille lire les logs du run.

| Table | Partitionnement | Unité d'alignement | Le job se défend seul |
|---|---|---|---|
| `bronze/*` (batch) | `year/month` | mois | oui — marker `_SUCCESS` |
| `silver/grid_load` | `year/month` | mois | non — couvert par le DAG |
| `silver/grid_generation` | `year/month` | mois | non — couvert par le DAG |
| `silver/weather` | `year/month` | mois | non — couvert par le DAG |
| `silver/_rejects/*` | `run_window` | la fenêtre du run | oui — la partition *est* la fenêtre |
| `gold/mix_horaire` | `year/month` (de `ts_utc`) | mois **UTC** | oui |
| `gold/kpi_daily` | `year/month` (de `date_local`) | mois **local** | oui |
| `gold/ml_features` | `year/month` (de `ts_utc`) | mois **UTC** | oui |

Les trois lignes Gold sont implémentées : `src/gold/windows.py` porte les
dérivations (`align_to_month`, `reading_window`) et `gold_build.py` les
applique. La ligne Silver est un **constat de conformité**, pas une correction
en attente : ses jobs écrivent la fenêtre qu'ils reçoivent, et l'invariant
tient tant que le DAG leur passe des bornes alignées.

`window_written` est matérialisée sous la forme de deux colonnes,
`window_start` et `window_end`, présentes dans les trois tables Gold. Pas
d'horodatage de build à côté : rejouer un mois clos doit redonner le même
fichier, et une colonne `built_at` suffirait à casser cette propriété.

### Mois UTC et mois local ne sont pas le même mois

`kpi_daily` agrège par journée locale : ses partitions sont des mois
**locaux**. Le 31 mars 23:00 UTC est déjà le 1er avril à Paris, donc borner
`kpi_daily` sur `ts_utc` y ferait entrer une journée d'avril isolée — et
l'écrasement dynamique remplacerait tout le mois d'avril par ce seul jour.
Le job borne donc `kpi_daily` sur `date_local` et les deux autres tables sur
`ts_utc`. C'est la même `window_written`, appliquée à la colonne qui porte
réellement le partitionnement de chaque table.

Corollaire à ne pas perdre de vue : `window_read` doit dépasser le mois local
des deux côtés, ce qu'elle fait déjà largement (8 jours en amont, 1 en aval),
sans quoi le premier et le dernier jour du mois seraient des journées
tronquées et leurs KPIs faux sans le dire.

Dernier point, qui n'est pas une faiblesse mais une propriété à énoncer :
l'alignement garantit que la partition écrite a été entièrement recalculée, pas
que les données sources soient complètes. Le mois courant est partiel par
nature — son dernier jour sera réécrit au run suivant. Rejouer un mois clos est
donc **idempotent** ; rejouer le mois courant est **convergent**.
