"""cit-HepPh (SNAP): red de citas de arXiv hep-ph -> parquet (edges, nodes, seeds).

Entradas : data/raw/cit-HepPh.txt         aristas citante -> citado (dirigido)
           data/raw/cit-HepPh-dates.txt   id <TAB> fecha de publicación
Salidas  : data/processed/hepph/{edges,nodes,seeds}.parquet

Detalles del dataset (comprobados con los archivos):
  - El archivo de aristas guarda los ids de arXiv SIN ceros a la izquierda (p. ej. `10088` es el
    paper `0010088`), mientras que el de fechas los trae rellenos a 7 dígitos. Se empareja con zfill(7).
  - El archivo de fechas termina en 2002 y no cubre ~4 000 papers de las aristas; para esos, el año
    se obtiene del propio id de arXiv (AAMMNNN): `0302202` -> 2003.
  - Los ids de 9 dígitos que empiezan con `11` son cross-listed (`11<id_real>`); se les quita el prefijo.
Tópicos (semillas del PageRank sensible a tópico): periodos de publicación (ver common/topics.py).
"""
import sys
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code (Run Python File o celdas `# %%`)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

import pandas as pd

from src.common.paths import HEPPH_DATES, HEPPH_EDGES, processed
from src.common.topics import hepph_topic


def arxiv_year(paper_id):
    """Año de publicación a partir del id numérico de arXiv (sin ceros a la izquierda)."""
    s = str(paper_id).zfill(7)
    if len(s) == 9 and s.startswith("11"):  # cross-listed
        s = s[2:]
    yy = int(s[:2])
    return 1900 + yy if yy >= 90 else 2000 + yy


def run(edges_path=HEPPH_EDGES, dates_path=HEPPH_DATES, out_dir=None):
    out = out_dir or processed("hepph")
    out.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(edges_path, sep=r"\s+", comment="#", names=["src", "dst"], dtype="int64")
    raw = raw[raw.src != raw.dst].drop_duplicates()

    papers = pd.Index(sorted(set(raw.src) | set(raw.dst)))
    edges = pd.DataFrame({"src": papers.get_indexer(raw.src), "dst": papers.get_indexer(raw.dst)})

    # Fechas: clave zfill(7); los cross-listed (11 + id) se normalizan quitando el prefijo
    dates = pd.read_csv(dates_path, sep=r"\s+", comment="#", names=["paper", "date"], dtype={"paper": str})
    dates["key"] = dates.paper.where(~((dates.paper.str.len() == 9) & dates.paper.str.startswith("11")),
                                     dates.paper.str[2:])
    dates["date"] = pd.to_datetime(dates.date, errors="coerce")
    date_by_key = dates.drop_duplicates("key").set_index("key").date

    nodes = pd.DataFrame({"id": range(len(papers)), "paper": papers.astype("int64")})
    nodes["year"] = nodes.paper.map(arxiv_year)
    # La fecha se guarda como texto AAAA-MM-DD (Spark 3 no lee timestamps de nanosegundos de pandas)
    nodes["date"] = nodes.paper.astype(str).str.zfill(7).map(date_by_key).dt.strftime("%Y-%m-%d")
    nodes["topic"] = nodes.year.map(hepph_topic)

    seeds = nodes.dropna(subset=["topic"])[["id", "topic"]].rename(columns={"id": "node"})

    edges.to_parquet(out / "edges.parquet", index=False)
    nodes.to_parquet(out / "nodes.parquet", index=False)
    seeds.to_parquet(out / "seeds.parquet", index=False)
    print(f"[hepph] nodos={len(nodes):,} aristas={len(edges):,} semillas={len(seeds):,}")
    print(seeds.topic.value_counts().sort_index().to_string())
    return out


if __name__ == "__main__":
    # %%
    run()
