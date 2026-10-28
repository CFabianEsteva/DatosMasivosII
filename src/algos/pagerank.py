"""PageRank en MapReduce sobre RDDs de PySpark (Leskovec et al., cap. 5.2).

Una iteración (ranks r, matriz de enlaces `links` particionada por hash):
  Map    : para cada (u, destinos) con rango r_u emite (v, r_u / deg(u)) por cada destino v
  Reduce : s_v = suma de las contribuciones que llegan a v           (reduceByKey)
  Update : r_v = beta * s_v + (1 - beta)/N + beta * D/N              (D = masa de nodos sin salida)
El teletransporte (1-beta) cubre las trampas de araña, y la masa D de los nodos colgantes se
redistribuye de forma uniforme, de modo que el vector siempre suma 1.
"""
import time
from operator import add

from src.common.graph_io import LEVEL


def _spread(kv):
    """Map: (u, (destinos, r_u)) -> (v, r_u/deg(u)) para cada destino."""
    dsts, r = kv[1]
    share = r / len(dsts)
    return ((d, share) for d in dsts)


def pagerank(graph, beta=0.85, tol=1e-8, max_iter=100, checkpoint_every=10, verbose=True):
    """Devuelve (ranks, history).

    ranks   : RDD (nodo, rango) persistido, suma 1
    history : lista con la distancia L1 entre iteraciones consecutivas (curva de convergencia)
    """
    P, N = graph.P, graph.N
    ranks = graph.nodes.mapValues(lambda _: 1.0 / N).persist(LEVEL)
    history = []
    t0 = time.time()

    for it in range(1, max_iter + 1):
        sums = graph.links.join(ranks, P).flatMap(_spread).reduceByKey(add, P)
        dangling = ranks.join(graph.dangling, P).map(lambda kv: kv[1][0]).sum()
        base = (1.0 - beta) / N + beta * dangling / N

        new = ranks.leftOuterJoin(sums, P).mapValues(
            lambda v: base + beta * (v[1] if v[1] is not None else 0.0)
        ).persist(LEVEL)
        if checkpoint_every and it % checkpoint_every == 0:
            new.checkpoint()

        delta = ranks.join(new, P).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum()  # fuerza el cómputo
        ranks.unpersist()
        ranks = new
        history.append(delta)
        if verbose:
            print(f"[PageRank] iter {it:3d}  L1={delta:.3e}  ({time.time() - t0:.0f}s)")
        if delta < tol:
            break
    return ranks, history
