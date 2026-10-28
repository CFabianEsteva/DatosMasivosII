# Plan de trabajo (versión 2 días) – Práctica 3, Datos Masivos II
PageRank · HITS · PageRank Sensible a Tópico sobre Wikipedia, cit-HepPh y Spotify, con MapReduce en PySpark.
**Entrega: mañana (29-oct-2026) 11:59 pm.**

## Contexto
Con el tiempo que queda se recorta el plan largo a lo esencial: los 3 algoritmos corriendo en los 3 datasets, con un reporte y un notebook de análisis. Los algoritmos se implementan en estilo MapReduce (Leskovec cap. 5) sobre PySpark `local[8]`. La "hash" del profesor se toma como el particionado por hash de la clave en el shuffle (`partitionBy`), que evita reordenar la matriz de enlaces en cada iteración. Se deja explícito en el código y en el reporte.

## Cambios acordados respecto al plan largo
- Se mantiene la estructura de proyecto original, **con notebooks**.
- **`requirements.txt` en lugar de `environment.yml`**: tú armas el entorno conda.
- **Todo se corre desde VS Code**, sin depender de la terminal. Los scripts de `src/` y `wiki_full/` llevan los parámetros (ruta, `MAX_BYTES`, umbrales) en constantes al inicio y celdas `# %%`, para correrlos con *Run Python File* o celda por celda. No se usan scripts `.sh`.
- **Se elimina la carpeta `tests/`.** La validación contra networkx se hace dentro de un script de `baselines/`.
- **Se elimina el barrido de β y el análisis de sensibilidad de la poda.** Se usa β = 0.85 fijo y un solo umbral de poda.
- **Wikipedia en dos etapas:** primero una **muestra** (prefijo del dump) para desarrollar y tener resultados seguros, y después los **117 GB completos** con los mismos scripts, lanzados en segundo plano (de noche). Si la corrida completa termina a tiempo, sus resultados se agregan al reporte. Si no, la muestra es el resultado entregado, y los scripts de la corrida completa quedan listos para ejecutarse.

## Estado de los datos (revisado en `Practica3/`)
- `enwiki.xml` (117 GB): XML completo y descomprimido, termina en `</mediawiki>`. De aquí se toma primero una muestra y luego el archivo completo.
- `spotify/edges.csv` (300 k aristas) y `nodes.csv` (156 k artistas con `genres`): listos. La red es no dirigida.
- `Cit-HepPh.txt` (421 578 aristas, 34 546 nodos, dirigido citante→citado): ya está en la carpeta. `snap.txt` es el archivo de fechas (`id<TAB>fecha`) y se usa para definir tópicos por época.
- Máquina: 8 núcleos, 15 GB RAM, 674 GB libres, Java 21 (necesario para Spark, ya instalado).

## Estructura del repo (nada se sube a GitHub por ahora)
```
Practica3/
  README.md  requirements.txt  .gitignore (data/ y results/raw)
  src/
    ingest/    parse_wiki.py  load_hepph.py  load_spotify.py   # parse_wiki.py usa MAX_BYTES (muestra) o None (completo)
    algos/     pagerank.py  hits.py  topic_pagerank.py   # MapReduce en PySpark
    common/    spark_session.py  graph_io.py  metrics.py
    baselines/ reference_networkx.py                      # validación en grafos chicos
  notebooks/   analisis_resultados.ipynb                  # muestra + HepPh + Spotify (y completo si termina)
  wiki_full/   01_ingest.py  02_graph.py  03_algos.py  README.md   # scripts con celdas `# %%` para toda enwiki, se corren desde VS Code
  data/        raw/ (enlaces simbólicos a los archivos actuales)  processed/ (parquet)
  results/     tablas CSV, figuras
  report/      informe.tex  (compila a informe.pdf)  referencias.bib  figuras/
```

## Cronograma

### Día 1 (hoy) – Infraestructura, algoritmos y datasets chicos
| Bloque | Tarea |
|---|---|
| 1. Arranque (~1 h) | Escribir `requirements.txt` (pyspark, pandas, pyarrow, networkx, scipy, matplotlib, ipykernel). **Tú creas el entorno conda** (Python 3.11 o 3.12; el 3.14 del sistema es muy nuevo para pyspark) e instalas con `pip install -r requirements.txt`, y eliges ese kernel en VS Code. Yo creo el esqueleto del repo, `.gitignore` y los enlaces simbólicos en `data/raw/`. |
| 2. Ingesta de Wikipedia (~1.5 h de escribir) | Un solo `parse_wiki.py`. Spark lee con `textinputformat.record.delimiter="</page>"` y reparte en los 8 núcleos. Por página extrae `title`, `ns`, `redirect`, enlaces `[[...]]` y `[[Category:...]]`. Salida en parquet por particiones. Prueba con ~1 GB para medir velocidad, y luego corre la **muestra** (prefijo de ~5–10 GB) en segundo plano mientras se hacen los algoritmos. |
| 3. Algoritmos (~3 h) | `pagerank.py`, `hits.py`, `topic_pagerank.py` en MapReduce: grafo como RDD `(nodo, destinos)` particionado por hash y cacheado. PageRank con β = 0.85, nodos colgantes redistribuidos, parada por L1 < 1e-8. HITS con normalización global por ronda. Sensible a Tópico con un vector de rango de dimensión T (el shuffle se comparte entre tópicos). |
| 4. Validación (~1 h) | Grafo de juguete del libro (A,B,C,D) y comparación contra `networkx` en Spotify y HepPh con `baselines/reference_networkx.py` (diferencia < 1e-6). |
| 5. Spotify + HepPh completos (~1.5 h) | Carga, limpieza (duplicados, auto-lazos), estadísticas, corrida de los 3 algoritmos. Tópicos de Spotify: `genres` → ~8 macro-géneros por palabras clave. Tópicos de HepPh: quinquenios por fecha de publicación. |

**Al final del día 1:** con la velocidad medida en la prueba, se lanza la **ingesta de los 117 GB completos** en segundo plano (`wiki_full/01_ingest.py`), con salida en parquet por particiones. Si el equipo se queda sin RAM o disco, se puede reanudar con lo ya procesado. Mientras corre, el resto del trabajo se hace con la muestra.

### Día 2 (mañana, hasta las 11:59 pm) – Wikipedia, resultados y reporte
| Bloque | Tarea |
|---|---|
| 1. Grafo de Wikipedia (~1.5 h) | Normalizar títulos, join por hash título→id, resolver redirects, y **podar**: solo namespace 0, sin redirects, aristas solo entre artículos de la muestra, y un umbral de in-degree (tipo k-core) con un único valor. Reportar conteos antes y después. |
| 2. Tópicos de Wikipedia (~1 h) | ~8–10 macro-tópicos (Ciencia, Deportes, Artes, Historia, Geografía, Política, Tecnología...) por mapeo de categorías a palabras clave. Semillas = artículos de ese tópico. |
| 3. Corridas en Wikipedia (~1 h) | PageRank, HITS y Sensible a Tópico sobre el grafo podado de la **muestra**. Si la ingesta completa ya terminó (punto de decisión: ~6 pm), se repiten los pasos 1–3 con `wiki_full/02_graph.py` y `03_algos.py` sobre todo enwiki, y se comparan contra la muestra. |
| 4. Notebook de análisis (~2 h) | `analisis_resultados.ipynb`: top-20 por algoritmo y dataset, top por tópico y ejemplo de combinación en consulta, PageRank vs HITS (Spearman y solapamiento del top-k), PageRank vs in-degree, distribución de grados y curvas de convergencia. |
| 5. Reporte (~2 h) | **Informe final en PDF hecho con LaTeX** (`report/informe.tex` → `informe.pdf`), con la misma plantilla de tu `Guia_Examen_Datos_Masivos.tex` (article 11pt, babel español, geometry, booktabs, hyperref). Contenido: introducción, método (los 3 algoritmos en MapReduce, el papel del hash), datasets y preprocesamiento (muestra y completo de Wikipedia, poda, tópicos), resultados con tablas y figuras que salen del notebook, discusión, limitaciones, conclusiones y bibliografía (Leskovec et al. 2016, cap. 5 y 10). `README.md` aparte, corto, con el orden de ejecución. `pdflatex`/`latexmk` ya están instalados, y se compila desde VS Code (extensión LaTeX Workshop). |
| Colchón | ~2 h para fallos o ajustes. |

## Notas técnicas a documentar
- Spotify es simétrico: HITS da hubs = autoridades y PageRank es casi proporcional al grado. Se menciona en el análisis.
- En la muestra de Wikipedia solo entran los artículos de un prefijo del dump (ordenado por ID de página) y sus enlaces entre sí. Los enlaces hacia fuera de la muestra se descartan. En ambas versiones, solo cuenta enlaces escritos en el wikitext, no los que generan las plantillas.
- Los IDs de HepPh que empiezan con `11` son cross-listed (según el comentario del archivo de fechas).

## Verificación
1. El grafo de juguete reproduce el resultado conocido, y la diferencia contra networkx es < 1e-6 en Spotify y HepPh.
2. Las masas de PageRank suman 1 y las normas de HITS son estables.
3. La ingesta de Wikipedia se prueba en ~1 GB, luego en la muestra, y solo después se lanza el archivo completo.
4. El notebook se ejecuta de arriba abajo, y el `README` indica en qué orden correr cada script o notebook desde VS Code.
5. **No se sube nada a GitHub** (`CFabianEsteva/DatosMasivosII`) sin tu confirmación explícita.

## Riesgos
- La corrida completa de 117 GB con regex en Python puede tardar varias horas. Por eso se lanza el día 1 en la noche y es opcional para la entrega. Al correr desde VS Code, hay que dejar el script abierto y la sesión del equipo activa (sin suspender) mientras dure la ingesta. El tiempo real se estima con la prueba de 1 GB.
- El grafo completo es grande (millones de nodos, cientos de millones de aristas). En 15 GB de RAM depende de la poda por in-degree y de `spark.local.dir` en el NVMe.

## Nota: aligerar todo a la hora de ir al grafo (Wikipedia)
Es el punto más pesado, así que al construir el grafo se busca la forma de reducir tamaño lo antes posible. Ideas a aplicar y a afinar cuando lleguemos ahí:
- **En la ingesta:** no guardar el texto de los artículos, solo `id`, `title`, `ns`, `redirect`, enlaces y categorías. Descartar en el mapper las páginas que no son namespace 0 y las que son redirects (guardando solo el par origen→destino de los redirects).
- **Enlaces:** deduplicar por `(origen, destino)`, quitar auto-lazos y enlaces a `File:`, `Category:`, `Template:`, etc. Mantener solo los destinos que existen como artículo.
- **IDs enteros** (int32/int64) en vez de títulos en las aristas. La tabla título→id se usa una sola vez, en el join, y después se descarta.
- **Poda por in-degree / k-core** con un solo umbral, para quedarse solo con los artículos que reciben enlaces. Es el recorte principal para "nodos sin relevancia".
- **Formato:** parquet con compresión (zstd/snappy), columnas mínimas, y particiones razonables (~64–200).
- **Memoria en las iteraciones:** `float32` para los rangos si hace falta, `persist` en disco (`MEMORY_AND_DISK`), checkpoint periódico para cortar el linaje y no cargar el grafo en el driver.
- **Opcional si no alcanza:** quedarse con los top-N por in-degree (por ejemplo 1–2 M) y trabajar el grafo inducido.

## Alcance de esta sesión (instrucción del usuario)
Solo **escribir el código**; **no ejecutar nada del proyecto** (ni Spark, ni la ingesta, ni los algoritmos, ni instalar paquetes). Pasos:
1. Exportar este plan a `Practica3/PLAN_DE_TRABAJO.md` (solo local, nada a GitHub).
2. Crear la estructura del repo: `requirements.txt`, `.gitignore`, `README.md`, `src/` (ingest, algos, common, baselines), `wiki_full/`, `notebooks/analisis_resultados.ipynb`, `report/informe.tex`.
3. Dejar `Practica3/checkpoint.md` con lo hecho, lo pendiente y cómo seguir (validar, correr, tópicos de Wikipedia, informe), para retomar más tarde.
