# Checkpoint – Práctica 3 (actualizado 2026-10-28)

**Entrega: 2026-10-29, 11:59 pm.** Nada se ha subido a GitHub (repo `CFabianEsteva/DatosMasivosII`); se pide confirmación antes.

## Estado: código escrito, NO ejecutado
Se escribió todo el código pero **no se ha corrido nada con Spark**. Lo único probado:
- `py_compile` de todos los `.py`, sin errores.
- `parse_record` (parser de páginas de Wikipedia) en Python puro sobre los primeros 64 MB del XML: 1 607 páginas ns 0
  (1 198 artículos + 409 redirects), mediana de 126 enlaces y ~9 categorías por artículo. Velocidad ~21 MB/s por núcleo.
  Con 8 núcleos, el dump completo debería tardar del orden de decenas de minutos a ~1–2 h (hay que medirlo con Spark).
- `informe.tex` compila (4 páginas, con marcas rojas `[PENDIENTE]`), y el código del notebook parsea.

## Hecho
- [x] `PLAN_DE_TRABAJO.md`, `README.md` (orden de ejecución), `requirements.txt`, `.gitignore`
- [x] `data/raw/` con enlaces simbólicos a los archivos originales (no se movió nada)
- [x] `src/common/`: paths, sesión Spark, `Graph` (RDDs particionados por hash), métricas, tópicos
- [x] `src/algos/`: `pagerank.py`, `hits.py`, `topic_pagerank.py`, `run_all.py`
- [x] `src/ingest/`: `load_hepph.py`, `load_spotify.py` (pandas), `parse_wiki.py`, `build_wiki_graph.py` (Spark)
- [x] `src/baselines/reference_networkx.py` (grafo de juguete 7/33, 5/33, 21/33 y comparación con networkx)
- [x] `wiki_full/01_ingest.py`, `02_graph.py`, `03_algos.py` + README
- [x] `notebooks/analisis_resultados.ipynb` (solo lee resultados, no usa Spark)
- [x] `report/informe.tex` + `referencias.bib` (esqueleto con métodos redactados, resultados pendientes)

## Hallazgos sobre los datos (ya reflejados en el código)
- `snap.txt` es el archivo de fechas; las aristas de HepPh están en `Cit-HepPh.txt` (421 578 aristas, 34 546 nodos).
- HepPh: el archivo de aristas guarda ids **sin ceros a la izquierda** (`10088` = paper `0010088`), el de fechas los trae
  a 7 dígitos. Solo ~21.7 k de 34.5 k nodos coinciden tal cual; con `zfill(7)` son ~30.5 k. El resto (papers de 2003) no
  tiene fecha, así que el año sale del propio id de arXiv. Tópicos de HepPh = periodos 1992–94, 95–97, 98–2000, 2001–03.
- Spotify: 102 `spotify_id` repetidos en `nodes.csv` (se queda el de más seguidores); 7 aristas a ids inexistentes
  (se descartan); las aristas no tienen pares recíprocos, así que se simetriza (dos aristas por colaboración).
- Wikipedia: el XML está completo (termina en `</mediawiki>`).

## Pendiente (en este orden)
1. **Tú:** crear el entorno conda (Python 3.11/3.12), `pip install -r requirements.txt`, elegir el intérprete en VS Code.
2. Correr `load_hepph.py` y `load_spotify.py`; revisar los conteos que imprimen.
3. Correr `run_all.py` con `RUN = ["hepph", "spotify"]` y luego `reference_networkx.py` (debe dar `OK` en el toy y errores < 1e-6).
   Aquí es donde probablemente aparezcan los primeros errores de PySpark: se corrigen y se sigue.
4. Probar `parse_wiki.py` con `MAX_BYTES = 1 * 1024**3` para medir la velocidad real; luego muestra (6 GB) y `build_wiki_graph.py`.
5. Con la velocidad medida, decidir si se lanza `wiki_full/01_ingest.py` (reanudable, dejar la PC despierta).
6. Correr `run_all.py` en `wiki_sample`; ajustar `MIN_INDEGREE` según `graph_stats.json` (y aligerar si hace falta, ver `02_graph.py`).
7. Ejecutar el notebook, rellenar los `\pend{}` del informe y compilarlo a PDF.

## Cosas a vigilar al correr (riesgos conocidos)
- `parse_wiki.py`: que `newAPIHadoopFile` parta el archivo en ~460 bloques de 256 MB y que las particiones fuera de `[a, b)` no lean datos.
- Los workers deben poder importar `src.*` (se fija `PYTHONPATH` en `get_spark`). Si da `ModuleNotFoundError`, reiniciar el kernel.
- Los joins enlaces–rangos deberían ser locales (mismo particionador, mismo `numPartitions = P`); si en la UI de Spark se ven shuffles
  grandes en cada iteración, revisar que siempre se pase `P` explícito. La corrección del resultado no depende de eso.
- Memoria: `driver_memory = 8g` (la PC tiene 15 GB). Bajar `PARTITIONS` o subir `MIN_INDEGREE` si el grafo de Wikipedia no cabe.
- La muestra es un prefijo del dump: pocos enlaces caen dentro de la muestra (en 64 MB solo ~1.3 % de los enlaces apuntaban a páginas
  del propio fragmento). Si el grafo de la muestra sale muy ralo, subir `MAX_BYTES` o ir directo al dump completo.
- Decisiones por revisar: `MIN_INDEGREE` (3 en la muestra, 10 en el completo), exclusión de "List of…" y desambiguaciones, lista de palabras
  clave de tópicos en `src/common/topics.py`.
