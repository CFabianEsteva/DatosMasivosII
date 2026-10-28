"""HITS (hubs y autoridades) en MapReduce sobre RDDs de PySpark (Leskovec et al., cap. 5.5).

Cada ronda son dos pasos MapReduce, ambos con el grafo particionado por hash:
  autoridad a_v = suma de h_u sobre los u con u -> v   (grafo `links`,  join con h)
  hub       h_u = suma de a_v sobre los v con u -> v   (grafo `rlinks`, join con a)
Después de cada paso se normaliza en L2 con una reducción global (suma de cuadrados).
"""
import math
import time
from operator import add

from src.common.graph_io import LEVEL


def _propagate(adj, vec, P):
    """adj: (k, [vecinos]); vec: (k, valor). Emite (vecino, valor) y suma por vecino."""
    return (
        adj.join(vec, P)
        .flatMap(lambda kv: ((d, kv[1][1]) for d in kv[1][0]))
        .reduceByKey(add, P)
    )


def _complete_and_normalize(nodes, raw, P, checkpoint=False):
    """Pone 0 a los nodos sin contribuciones y normaliza el vector completo a norma L2 = 1."""
    full = nodes.leftOuterJoin(raw, P).mapValues(lambda v: v[1] if v[1] is not None else 0.0).persist(LEVEL)
    norm = math.sqrt(full.values().map(lambda x: x * x).sum()) or 1.0
    out = full.mapValues(lambda x: x / norm).persist(LEVEL)
    if checkpoint:
        out.checkpoint()
    out.count()
    full.unpersist()
    return out


def hits(graph, tol=1e-8, max_iter=100, checkpoint_every=10, verbose=True):
    """Devuelve (authority, hub, history). Ambos vectores son RDD (nodo, valor) con norma L2 = 1.

    history: lista con L1(a_t - a_{t-1}) + L1(h_t - h_{t-1}) por iteración.
    """
    P = graph.P
    hub = _complete_and_normalize(graph.nodes, graph.nodes.mapValues(lambda _: 1.0), P)
    auth = None
    history = []
    t0 = time.time()

    for it in range(1, max_iter + 1):
        ckpt = bool(checkpoint_every) and it % checkpoint_every == 0
        new_auth = _complete_and_normalize(graph.nodes, _propagate(graph.links, hub, P), P, ckpt)
        new_hub = _complete_and_normalize(graph.nodes, _propagate(graph.rlinks, new_auth, P), P, ckpt)

        d_hub = hub.join(new_hub, P).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum()
        d_auth = 0.0 if auth is None else auth.join(new_auth, P).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum()
        delta = d_hub + d_auth

        if auth is not None:
            auth.unpersist()
        hub.unpersist()
        auth, hub = new_auth, new_hub
        history.append(delta)
        if verbose:
            print(f"[HITS] iter {it:3d}  L1={delta:.3e}  ({time.time() - t0:.0f}s)")
        if delta < tol:
            break
    return auth, hub, history
