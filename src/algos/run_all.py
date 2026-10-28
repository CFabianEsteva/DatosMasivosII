"""Corre PageRank, HITS y PageRank Sensible a Tópico sobre un dataset ya procesado.

Entrada : data/processed/<name>/{edges,nodes,seeds}.parquet
Salida  : results/<name>/ranks.parquet  una fila por nodo: id, pagerank, authority, hub, tspr_<topico>...
          results/<name>/run_info.json  parámetros, tamaños, iteraciones, tiempos y curvas de convergencia
Los títulos/nombres de los nodos se unen después (en el notebook) con nodes.parquet por `id`.
"""
import json
import re
import sys
import time
from pathlib import Path

try:  # permite correr este archivo directo desde VS Code (Run Python File o celdas `# %%`)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
except NameError:
    sys.path.insert(0, str(Path.cwd()))

from src.algos.hits import hits
from src.algos.pagerank import pagerank
from src.algos.topic_pagerank import topic_pagerank
from src.common.graph_io import Graph, read_edges, read_node_ids
from src.common.paths import processed, results

# ----------------------------------------------------------------------------- parámetros
BETA = 0.85          # factor de amortiguamiento (fijo; no se barre)
TOL = 1e-8
MAX_ITER = 100
RUN = ["hepph", "spotify"]   # "wiki_sample" y "wiki_full" se agregan cuando estén listos
PARTITIONS = {"hepph": 16, "spotify": 16, "wiki_sample": 64, "wiki_full": 128}


def run_dataset(spark, name, beta=BETA, tol=TOL, max_iter=MAX_ITER, partitions=None):
    src_dir, out_dir = processed(name), results(name)
    out_dir.mkdir(parents=True, exist_ok=True)
    P = partitions or PARTITIONS.get(name, 64)
    t0 = time.time()
    info = {"dataset": name, "beta": beta, "tol": tol, "max_iter": max_iter, "particiones": P}

    edges = read_edges(spark, src_dir / "edges.parquet")
    node_ids = read_node_ids(spark, src_dir / "nodes.parquet")
    graph = Graph(edges, nodes=node_ids, num_partitions=P)
    info.update(nodos=graph.N, aristas=graph.num_edges(), nodos_sin_salida=graph.num_dangling())
    print(f"[{name}] nodos={info['nodos']:,} aristas={info['aristas']:,} sin_salida={info['nodos_sin_salida']:,}")

    # --- PageRank
    t = time.time()
    pr, pr_hist = pagerank(graph, beta=beta, tol=tol, max_iter=max_iter)
    info["pagerank"] = {"iteraciones": len(pr_hist), "segundos": round(time.time() - t, 1), "historia": pr_hist}

    # --- HITS
    t = time.time()
    auth, hub, hits_hist = hits(graph, tol=tol, max_iter=max_iter)
    info["hits"] = {"iteraciones": len(hits_hist), "segundos": round(time.time() - t, 1), "historia": hits_hist}

    # --- PageRank sensible a tópico
    seeds_df = spark.read.parquet(str(src_dir / "seeds.parquet"))
    topics = sorted(r["topic"] for r in seeds_df.select("topic").distinct().collect())
    t_index = {tp: i for i, tp in enumerate(topics)}
    seeds = seeds_df.rdd.map(lambda r: (r["node"], t_index[r["topic"]]))
    t = time.time()
    ts, topics_used, ts_hist = topic_pagerank(graph, seeds, topics, beta=beta, tol=tol, max_iter=max_iter)
    info["tspr"] = {"topicos": topics_used, "iteraciones": len(ts_hist),
                    "segundos": round(time.time() - t, 1), "historia": ts_hist}

    # --- Un solo parquet con todos los vectores
    cols = ["tspr_" + re.sub(r"\W", "_", tp) for tp in topics_used]
    pr_df = pr.map(lambda kv: (kv[0], float(kv[1]))).toDF(["id", "pagerank"])
    ha_df = (auth.join(hub, P)
             .map(lambda kv: (kv[0], float(kv[1][0]), float(kv[1][1])))
             .toDF(["id", "authority", "hub"]))
    ts_df = ts.map(lambda kv: (kv[0],) + tuple(float(x) for x in kv[1])).toDF(["id"] + cols)
    ranks = pr_df.join(ha_df, "id").join(ts_df, "id")
    ranks.write.mode("overwrite").parquet(str(out_dir / "ranks.parquet"))

    info["segundos_total"] = round(time.time() - t0, 1)
    (out_dir / "run_info.json").write_text(json.dumps(info, indent=2, ensure_ascii=False))
    print(f"[{name}] listo en {info['segundos_total']}s -> {out_dir}")
    graph.unpersist()
    return info


if __name__ == "__main__":
    # %%
    from src.common.spark_session import get_spark

    spark = get_spark("practica3-algos", shuffle_partitions=64)
    # %%
    for dataset in RUN:
        run_dataset(spark, dataset)
