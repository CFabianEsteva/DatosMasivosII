"""Rutas del proyecto. Todo es relativo a la raíz de Practica3/."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
RESULTS = ROOT / "results"

# Archivos originales (enlaces simbólicos en data/raw/)
ENWIKI_XML = DATA_RAW / "enwiki.xml"
HEPPH_EDGES = DATA_RAW / "cit-HepPh.txt"
HEPPH_DATES = DATA_RAW / "cit-HepPh-dates.txt"
SPOTIFY_DIR = DATA_RAW / "spotify"


def processed(name):
    """data/processed/<name>/ con edges.parquet, nodes.parquet y seeds.parquet."""
    return DATA_PROCESSED / name


def results(name):
    """results/<name>/ con ranks.parquet y run_info.json."""
    return RESULTS / name
