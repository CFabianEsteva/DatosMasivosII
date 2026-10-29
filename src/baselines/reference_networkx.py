"""Validación de los algoritmos MapReduce contra implementaciones de referencia (networkx).

1. `toy_check`      : grafo de 3 nodos del libro (Leskovec et al., Ej. 5.? con trampa de araña y beta=0.8)
                      cuyo PageRank exacto es (7/33, 5/33, 21/33).
2. `compare_dataset`: compara results/<name>/ranks.parquet contra networkx en hepph y spotify
                      (PageRank, HITS y el PageRank personalizado del primer tópico).
networkx es lento para millones de nodos, así que solo se usa en los datasets chicos.
Para comparar HITS: networkx normaliza a suma 1 y aquí a norma L2, por eso se reescala a suma 1.
"""
import sys
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code (Run Python File o celdas `# %%`)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

import numpy as np
import pandas as pd

from src.algos.pagerank import pagerank
from src.common.graph_io import Graph
from src.common.metrics import max_abs_diff, spearman
from src.common.paths import processed, results

BETA = 0.85
TOL_OK = 1e-6


def toy_check(spark):
    """y->{y,a}, a->{y,m}, m->{m}; con beta=0.8 el PageRank es (7/33, 5/33, 21/33)."""
    graph = Graph.from_pairs(spark, [(0, 0), (0, 1), (1, 0), (1, 2), (2, 2)], N=3, num_partitions=2)
    ranks, _ = pagerank(graph, beta=0.8, tol=1e-13, max_iter=500, verbose=False)
    got = {i: float(v) for i, v in enumerate(ranks)}
    expected = {0: 7 / 33, 1: 5 / 33, 2: 21 / 33}
    err = max(abs(got[k] - expected[k]) for k in expected)
    print(f"[toy] obtenido={ {k: round(v, 6) for k, v in got.items()} }  esperado={ {k: round(v, 6) for k, v in expected.items()} }")
    print(f"[toy] error máximo = {err:.2e}  ->  {'OK' if err < TOL_OK else 'FALLA'}")
    graph.unpersist()
    return err


def _load(name):
    src, res = processed(name), results(name)
    edges = pd.read_parquet(src / "edges.parquet")
    nodes = pd.read_parquet(src / "nodes.parquet", columns=["id"])
    seeds = pd.read_parquet(src / "seeds.parquet")
    ranks = pd.read_parquet(res / "ranks.parquet").set_index("id").sort_index()
    return edges, nodes, seeds, ranks


def compare_dataset(name, beta=BETA):
    import networkx as nx

    edges, nodes, seeds, ranks = _load(name)
    G = nx.DiGraph()
    G.add_nodes_from(nodes.id.tolist())
    G.add_edges_from(zip(edges.src.tolist(), edges.dst.tolist()))
    idx = ranks.index

    report = {}
    # PageRank (networkx reparte la masa de nodos colgantes de forma uniforme, igual que aquí)
    ref = pd.Series(nx.pagerank(G, alpha=beta, tol=1e-12, max_iter=1000)).reindex(idx)
    report["pagerank"] = (max_abs_diff(ref, ranks.pagerank), spearman(ref, ranks.pagerank))

    # HITS (networkx devuelve (hubs, autoridades) con suma 1)
    h_ref, a_ref = nx.hits(G, max_iter=2000, tol=1e-12, normalized=True)
    h_ref, a_ref = pd.Series(h_ref).reindex(idx), pd.Series(a_ref).reindex(idx)
    h_mine, a_mine = ranks.hub / ranks.hub.sum(), ranks.authority / ranks.authority.sum()
    report["hub"] = (max_abs_diff(h_ref, h_mine), spearman(h_ref, h_mine))
    report["authority"] = (max_abs_diff(a_ref, a_mine), spearman(a_ref, a_mine))

    # PageRank personalizado del primer tópico (equivale a una columna del PageRank sensible a tópico)
    topic = sorted(seeds.topic.unique())[0]
    col = "tspr_" + "".join(c if c.isalnum() or c == "_" else "_" for c in topic)
    s = seeds[seeds.topic == topic].node.unique()
    pers = {int(n): 1.0 / len(s) for n in s}
    ref_t = pd.Series(nx.pagerank(G, alpha=beta, personalization=pers, tol=1e-12, max_iter=1000)).reindex(idx)
    report[f"tspr[{topic}]"] = (max_abs_diff(ref_t, ranks[col]), spearman(ref_t, ranks[col]))

    print(f"=== {name}: nodos={G.number_of_nodes():,} aristas={G.number_of_edges():,}")
    for k, (err, rho) in report.items():
        print(f"  {k:<28s} max|diff|={err:.2e}  Spearman={rho:.6f}  {'OK' if err < TOL_OK else 'REVISAR'}")
    return report


if __name__ == "__main__":
    # %%
    from src.common.spark_session import get_spark

    spark = get_spark("practica3-validacion", cores=4, driver_memory="2g", shuffle_partitions=8)
    toy_check(spark)
    # %% (requiere haber corrido antes src/algos/run_all.py para hepph y spotify)
    for dataset in ("hepph", "spotify"):
        compare_dataset(dataset)
