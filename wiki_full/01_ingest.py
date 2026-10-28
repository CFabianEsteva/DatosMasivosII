"""Wikipedia COMPLETA, paso 1: enwiki.xml (117 GB) -> data/processed/wiki_full/pages/.

Corre desde VS Code (Run Python File, o celda por celda con `# %%`). Es el mismo código que la
muestra (src/ingest/parse_wiki.py) pero sin límite de bytes.

- Se escribe por bloques de ~6 GB en data/processed/wiki_full/pages/part_NNNN/. Si se interrumpe,
  vuelve a correr este archivo: los bloques con `_SUCCESS` se omiten (RESUME = True).
- Deja la computadora despierta (sin suspender) y este script abierto mientras corre.
- Antes de lanzar todo, mide la velocidad con la muestra y estima el tiempo total
  (~117 GB / velocidad medida). Si va muy lento, baja a MAX_BYTES = 40 * 1024**3 (primeros 40 GB).
"""
import sys
from pathlib import Path

try:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.common.paths import processed
from src.common.spark_session import get_spark
from src.ingest.parse_wiki import run_ingest

# %% parámetros
OUT_NAME = "wiki_full"
MAX_BYTES = None      # None = todo el archivo
RESUME = True

# %% Spark
spark = get_spark("wiki-full-ingest", shuffle_partitions=64)

# %% ingesta (larga)
run_ingest(spark, out_dir=processed(OUT_NAME), max_bytes=MAX_BYTES, resume=RESUME)
