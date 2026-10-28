# Unidad 3 - Hits, PageRank y Sensible a tópico

**Equipo:**
- Esteva Gallegos Carlos Fabián
- León Navarrete Adam Edmundo
- Munive Ramírez Ibrahim

Práctica 3 de Datos Masivos II (Unidad 3, análisis de vínculos). Se implementan desde cero los algoritmos
PageRank, HITS y PageRank Sensible a Tópico con el paradigma MapReduce sobre PySpark, y se aplican a tres redes
reales de tamaños muy distintos para comparar qué nodos resultan más importantes según cada algoritmo.

Los tres algoritmos se aplican a:

| Dataset | Origen | Grafo |
|---|---|---|
| cit-HepPh | SNAP | citas entre papers de hep-ph (dirigido) |
| Spotify | Kaggle | colaboraciones entre artistas (no dirigido, se simetriza) |
| Wikipedia | `enwiki.xml` (117 GB) | enlaces entre artículos, podado por grado de entrada |

Plan completo: [`PLAN_DE_TRABAJO.md`](PLAN_DE_TRABAJO.md). Estado actual y pendientes: [`checkpoint.md`](checkpoint.md).

## Preparación
1. Crear un entorno conda con Python 3.11 o 3.12 y `pip install -r requirements.txt`. Hace falta Java 17 o 21.
2. En VS Code, elegir ese intérprete (y como kernel para el notebook).
3. Los archivos originales están enlazados en `data/raw/` (`enwiki.xml`, `cit-HepPh.txt`, `cit-HepPh-dates.txt`, `spotify/`).

## Orden de ejecución (todo desde VS Code: *Run Python File* o celda por celda con `# %%`)
| Paso | Archivo | Salida |
|---|---|---|
| 1 | `src/ingest/load_hepph.py` | `data/processed/hepph/` |
| 2 | `src/ingest/load_spotify.py` | `data/processed/spotify/` |
| 3 | `src/algos/run_all.py` (`RUN = ["hepph", "spotify"]`) | `results/hepph/`, `results/spotify/` |
| 4 | `src/baselines/reference_networkx.py` | validación contra networkx (imprime errores) |
| 5 | `src/ingest/parse_wiki.py` (muestra, `MAX_BYTES`) | `data/processed/wiki_sample/pages/` |
| 6 | `src/ingest/build_wiki_graph.py` | `data/processed/wiki_sample/{edges,nodes,seeds}.parquet` |
| 7 | `src/algos/run_all.py` con `RUN = ["wiki_sample"]` | `results/wiki_sample/` |
| 8 | `wiki_full/01_ingest.py`, `02_graph.py`, `03_algos.py` | dump completo (ver `wiki_full/README.md`) |
| 9 | `notebooks/analisis_resultados.ipynb` | tablas en `results/tablas/`, figuras en `report/figuras/` |
| 10 | `report/informe.tex` → `informe.pdf` (`latexmk -pdf` o LaTeX Workshop) | informe final |

## Estructura
```
src/common/     paths, sesión de Spark, grafo particionado por hash (graph_io), métricas, tópicos
src/ingest/     load_hepph.py  load_spotify.py  parse_wiki.py  build_wiki_graph.py
src/algos/      pagerank.py  hits.py  topic_pagerank.py  run_all.py
src/baselines/  reference_networkx.py
notebooks/      analisis_resultados.ipynb
wiki_full/      scripts para toda enwiki
report/         informe.tex  referencias.bib  figuras/
```

## La parte "hash" del MapReduce
La lista de adyacencia `(u, [destinos])` se particiona una sola vez por hash de la clave y se cachea. Los
vectores de rango se particionan igual, por lo que el join enlaces–rangos no mueve datos; solo viajan las
contribuciones por arista en el shuffle del reduce. Además, en Wikipedia cada título se identifica con un
hash de 64 bits, así que las aristas guardan hashes y no hace falta un join título→id.
