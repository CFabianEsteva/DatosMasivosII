"""Etapa 1 de Wikipedia: enwiki.xml (117 GB) -> parquet con una fila por artículo/redirect.

Cómo se reparte el trabajo (MapReduce nativo de Spark/Hadoop, sin cargar el XML en memoria):
  - Hadoop `TextInputFormat` con delimitador de registro `</page>`: cada registro es una página
    completa, y el archivo se parte en bloques de `SPLIT_MB` que se reparten entre los 8 núcleos.
  - El "map" es `parse_record`: regex sobre el texto de la página (sin parser XML, es mucho más rápido).

Cómo se aligera desde la ingesta (ver PLAN_DE_TRABAJO.md, "aligerar todo"):
  - Solo se guardan páginas del espacio de nombres 0 (artículos y redirects). Nada de texto.
  - Cada título se identifica con un hash de 64 bits (blake2b) y las aristas guardan hashes
    en vez de strings. Así no hace falta un join título->id sobre cientos de millones de enlaces.
  - Los enlaces de cada página se guardan como arreglo de hashes ya deduplicado, sin auto-lazos
    y sin enlaces a otros espacios de nombres (File:, Category:, Template:, ...).

Salida (parquet, un directorio por bloque para poder reanudar): <out_dir>/pages/part_NNNN/
  h BIGINT, page_id BIGINT, title STRING, redirect_h BIGINT (nulo si no es redirect),
  links ARRAY<BIGINT>, cats ARRAY<STRING>

Para la muestra se usa MAX_BYTES (prefijo del dump, ordenado por id de página); para el dump
completo, MAX_BYTES = None. Es el mismo código.
"""
import hashlib
import html
import math
import re
import sys
import time
from functools import lru_cache
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code (Run Python File o celdas `# %%`)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.common.paths import ENWIKI_XML, processed

# ----------------------------------------------------------------------------- parámetros
MAX_BYTES = 6 * 1024**3   # muestra: ~6 GB del inicio del dump. None = todo el archivo
OUT_NAME = "wiki_sample"  # data/processed/<OUT_NAME>/pages/
SPLIT_MB = 256            # tamaño de bloque de lectura (una partición de Spark por bloque)
CHUNK_PARTS = 24          # bloques por escritura (~6 GB); cada escritura es reanudable

# ----------------------------------------------------------------------------- parseo de una página
RE_TITLE = re.compile(r"<title>(.*?)</title>", re.S)
RE_NS = re.compile(r"<ns>(\d+)</ns>")
RE_ID = re.compile(r"<id>(\d+)</id>")
RE_REDIRECT = re.compile(r'<redirect title="([^"]*)"')
RE_TEXT = re.compile(r"<text[^>]*>(.*?)</text>", re.S)
RE_LINK = re.compile(r"\[\[([^\[\]|\n#]+)")
RE_CAT = re.compile(r"\[\[\s*category\s*:\s*([^\]|\n]+)", re.I)

# Prefijos de otros espacios de nombres / proyectos: esos enlaces no son artículos
NS_PREFIXES = {
    "file", "image", "media", "category", "template", "talk", "user", "user talk", "wikipedia", "wp",
    "project", "help", "portal", "draft", "module", "mediawiki", "special", "wiktionary", "wikt",
    "commons", "c", "w", "wikipedia talk", "file talk", "template talk", "category talk", "help talk",
    "portal talk", "draft talk", "module talk", "mediawiki talk", "book", "topic", "timedtext",
}


def norm_title(t):
    """Normaliza como MediaWiki (case=first-letter): '_'->' ', espacios colapsados, 1ª letra mayúscula."""
    t = t.replace("_", " ").strip()
    if "  " in t:
        t = " ".join(t.split())
    if t[:1].islower():
        t = t[0].upper() + t[1:]
    return t


def h64(s):
    """Hash de 64 bits con signo (cabe en BIGINT) de un título ya normalizado."""
    return int.from_bytes(hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest(), "big", signed=True)


@lru_cache(maxsize=500_000)
def target_hash(raw):
    """Texto de un enlace [[raw|...]] -> hash del título destino, o None si no es un artículo posible."""
    if "&" in raw:
        raw = html.unescape(raw)
    t = raw.strip()
    if not t or t[0] == ":":
        return None
    colon = t.find(":")
    if colon > 0 and t[:colon].strip().lower().replace("_", " ") in NS_PREFIXES:
        return None
    return h64(norm_title(t))


def parse_record(rec):
    """Registro (texto hasta `</page>`) -> tupla (h, page_id, title, redirect_h, links, cats) o None."""
    i = rec.find("<page>")
    if i < 0:  # cabecera/siteinfo suelta o cierre </mediawiki>
        return None
    rec = rec[i:]
    m = RE_NS.search(rec)
    if m is None or m.group(1) != "0":
        return None
    title = html.unescape(RE_TITLE.search(rec).group(1))
    page_id = int(RE_ID.search(rec).group(1))
    h = h64(norm_title(title))

    r = RE_REDIRECT.search(rec)
    if r is not None:
        rh = target_hash(r.group(1).split("#")[0])
        if rh is None or rh == h:
            return None  # redirect a otro espacio de nombres o a sí mismo
        return (h, page_id, title, rh, [], [])

    m = RE_TEXT.search(rec)
    text = m.group(1) if m else ""
    links = set()
    for raw in RE_LINK.findall(text):
        th = target_hash(raw)
        if th is not None and th != h:
            links.add(th)
    cats = {norm_title(html.unescape(c)) for c in RE_CAT.findall(text)}
    return (h, page_id, title, None, sorted(links), sorted(cats))


def _parse_partition(records):
    for _, rec in records:
        row = parse_record(rec)
        if row is not None:
            yield row


# ----------------------------------------------------------------------------- Spark
def run_ingest(spark, xml_path=ENWIKI_XML, out_dir=None, max_bytes=MAX_BYTES, split_mb=SPLIT_MB,
               chunk_parts=CHUNK_PARTS, resume=True):
    from pyspark.sql.types import (ArrayType, LongType, StringType, StructField, StructType)

    schema = StructType([
        StructField("h", LongType(), False),
        StructField("page_id", LongType(), False),
        StructField("title", StringType(), False),
        StructField("redirect_h", LongType(), True),
        StructField("links", ArrayType(LongType()), False),
        StructField("cats", ArrayType(StringType()), False),
    ])
    out = Path(out_dir) if out_dir else processed(OUT_NAME)
    pages_dir = out / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    sc = spark.sparkContext
    split = split_mb * 1024 * 1024
    conf = {
        "textinputformat.record.delimiter": "</page>",
        # minsize = maxsize para que el bloque sea de SPLIT_MB (el sistema de archivos local usa 32 MB)
        "mapreduce.input.fileinputformat.split.maxsize": str(split),
        "mapreduce.input.fileinputformat.split.minsize": str(split),
    }
    rdd = sc.newAPIHadoopFile(
        "file://" + str(Path(xml_path).resolve()),
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf=conf,
    )
    n_total = rdd.getNumPartitions()
    n_use = n_total if max_bytes is None else min(n_total, math.ceil(max_bytes / split))
    print(f"[ingest] bloques totales={n_total}, a procesar={n_use} "
          f"(~{n_use * split_mb / 1024:.1f} GB), de {chunk_parts} en {chunk_parts}")

    t_start = time.time()
    for ci, a in enumerate(range(0, n_use, chunk_parts)):
        b = min(a + chunk_parts, n_use)
        target = pages_dir / f"part_{ci:04d}"
        if resume and (target / "_SUCCESS").exists():
            print(f"[ingest] bloques {a}-{b - 1}: ya procesados, se omiten")
            continue
        t0 = time.time()
        # Las particiones fuera de [a, b) devuelven un iterador vacío sin leerse (no cuestan I/O)
        sel = rdd.mapPartitionsWithIndex(
            lambda i, it, a=a, b=b: _parse_partition(it) if a <= i < b else iter(())
        )
        spark.createDataFrame(sel, schema, verifySchema=False).write.mode("overwrite").parquet(str(target))
        print(f"[ingest] bloques {a}-{b - 1} listos en {time.time() - t0:.0f}s "
              f"(acumulado {(time.time() - t_start) / 60:.1f} min)")
    return out


if __name__ == "__main__":
    # %%
    from src.common.spark_session import get_spark

    spark = get_spark("wiki-ingest", shuffle_partitions=64)
    # %%
    run_ingest(spark)
