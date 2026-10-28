"""Spotify artist collaboration network (Kaggle) -> parquet (edges, nodes, seeds).

Entradas : data/raw/spotify/nodes.csv   spotify_id, name, followers, popularity, genres, chart_hits
           data/raw/spotify/edges.csv   id_0, id_1  (cada colaboración aparece UNA vez: red no dirigida)
Salidas  : data/processed/spotify/{edges,nodes,seeds}.parquet

Decisiones (comprobadas en los archivos):
  - 102 `spotify_id` aparecen repetidos en nodes.csv con distinto número de seguidores: se conserva
    la fila con más seguidores.
  - 7 aristas apuntan a ids que no están en nodes.csv: se descartan. No hay auto-lazos.
  - La red es no dirigida: cada colaboración se convierte en dos aristas dirigidas (u->v y v->u).
    Consecuencia a documentar: en un grafo simétrico, HITS da hubs = autoridades y PageRank queda
    casi proporcional al grado.
  - Se conservan los artistas sin colaboraciones (nodos aislados).
Tópicos: macro-géneros por palabra clave sobre `genres` (ver common/topics.py).
"""
import ast
import sys
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code (Run Python File o celdas `# %%`)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

import pandas as pd

from src.common.paths import SPOTIFY_DIR, processed
from src.common.topics import SPOTIFY_TOPICS, compile_topics, match_topics


def parse_genres(s):
    try:
        g = ast.literal_eval(s) if isinstance(s, str) else []
        return [str(x) for x in g] if isinstance(g, (list, tuple)) else []
    except (ValueError, SyntaxError):
        return []


def run(spotify_dir=SPOTIFY_DIR, out_dir=None):
    out = out_dir or processed("spotify")
    out.mkdir(parents=True, exist_ok=True)

    nodes = pd.read_csv(Path(spotify_dir) / "nodes.csv")
    nodes = (nodes.sort_values("followers", ascending=False)
             .drop_duplicates("spotify_id").sort_index().reset_index(drop=True))
    nodes.insert(0, "id", range(len(nodes)))
    idx = pd.Series(nodes.id.values, index=nodes.spotify_id.values)

    raw = pd.read_csv(Path(spotify_dir) / "edges.csv")
    a, b = raw.id_0.map(idx), raw.id_1.map(idx)
    ok = a.notna() & b.notna() & (a != b)
    a, b = a[ok].astype("int64"), b[ok].astype("int64")
    edges = pd.concat([pd.DataFrame({"src": a, "dst": b}), pd.DataFrame({"src": b, "dst": a})])
    edges = edges.drop_duplicates().reset_index(drop=True)

    # Semillas por macro-género
    compiled = compile_topics(SPOTIFY_TOPICS)
    rows = []
    for node_id, genres in zip(nodes.id, nodes.genres.map(parse_genres)):
        for t in match_topics(genres, compiled):
            rows.append((node_id, t))
    seeds = pd.DataFrame(rows, columns=["node", "topic"])

    nodes[["id", "spotify_id", "name", "followers", "popularity", "genres"]].to_parquet(
        out / "nodes.parquet", index=False)
    edges.to_parquet(out / "edges.parquet", index=False)
    seeds.to_parquet(out / "seeds.parquet", index=False)
    print(f"[spotify] nodos={len(nodes):,} aristas dirigidas={len(edges):,} semillas={len(seeds):,}")
    print(seeds.topic.value_counts().to_string())
    return out


if __name__ == "__main__":
    # %%
    run()
