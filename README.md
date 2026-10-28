# Unidad 3 - Hits, PageRank y Sensible a tópico

**Equipo:**
- Esteva Gallegos Carlos Fabián
- León Navarrete Adam Edmundo
- Munive Ramírez Ibrahim

Práctica 3 de Datos Masivos II (Unidad 3, análisis de vínculos). Se implementan desde cero los algoritmos
PageRank, HITS y PageRank Sensible a Tópico con el paradigma MapReduce sobre PySpark, y se aplican a tres redes
reales de tamaños muy distintos para comparar qué nodos resultan más importantes según cada algoritmo.

Los tres algoritmos se aplican a:

| Dataset | Origen | URL |
|---|---|---|
| cit-HepPh | SNAP | https://snap.stanford.edu/data/cit-HepPh.html |
| Spotify | Kaggle | https://www.kaggle.com/datasets/jfreyberg/spotify-artist-feature-collaboration-network/ |
| Wikipedia | `enwiki.xml | https://dumps.wikimedia.org/enwiki/latest/ |


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


