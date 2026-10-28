"""PageRank Sensible a Tópico (Haveliwala, 2002; Leskovec et al., cap. 5.3) en MapReduce.

Para cada tópico t con conjunto semilla S_t, el teletransporte salta solo a S_t:
    r_t = beta * M r_t + (1 - beta) * e_{S_t} / |S_t|
Se calculan los T vectores a la vez: el rango de cada nodo es un arreglo numpy de dimensión T, así
que todo el shuffle (las contribuciones por arista) se comparte entre tópicos. La masa de los nodos
colgantes de cada tópico también vuelve a teletransportarse a S_t, de modo que cada r_t suma 1.
En el momento de la consulta se combinan con pesos: r = sum_t w_t * r_t (ver `combine_topics`).
"""
import time
from operator import add

import numpy as np

from src.common.graph_io import LEVEL


def _spread(kv):
    dsts, r = kv[1]
    share = r / len(dsts)
    return ((d, share) for d in dsts)


def topic_pagerank(graph, seeds, topics, beta=0.85, tol=1e-8, max_iter=100,
                   checkpoint_every=10, dtype=np.float64, verbose=True):
    """Devuelve (ranks, topics_used, history).

    seeds       : RDD (nodo, indice_de_topico) con indices sobre la lista `topics`
    topics      : lista de nombres de tópico
    ranks       : RDD (nodo, ndarray de dimensión len(topics_used))
    topics_used : tópicos que sí tienen semillas dentro del grafo (en el orden de las columnas)
    history     : máximo (sobre tópicos) de la distancia L1 entre iteraciones
    """
    P = graph.P
    # Solo semillas que existen en el grafo (la poda pudo quitar algunas)
    seeds_in = (
        seeds.distinct()
        .map(lambda s: (s[0], s[1]))
        .join(graph.nodes, P)
        .map(lambda kv: (kv[0], kv[1][0]))
    )
    sizes = seeds_in.map(lambda s: (s[1], 1)).reduceByKey(add).collectAsMap()
    used = [i for i in range(len(topics)) if sizes.get(i, 0) > 0]
    if len(used) < len(topics):
        missing = [topics[i] for i in range(len(topics)) if i not in used]
        print(f"[TSPR] aviso: tópicos sin semillas en el grafo, se omiten: {missing}")
    if not used:
        raise ValueError("Ningún tópico tiene semillas dentro del grafo")
    remap = {old: new for new, old in enumerate(used)}
    topics_used = [topics[i] for i in used]
    T = len(used)
    inv_size = np.array([1.0 / sizes[i] for i in used], dtype=dtype)

    def onehot(s):
        v = np.zeros(T, dtype=dtype)
        v[remap[s[1]]] = inv_size[remap[s[1]]]
        return (s[0], v)

    tele = (
        seeds_in.filter(lambda s: s[1] in remap).map(onehot).reduceByKey(add, P)
    )
    # Vector de teletransporte para todos los nodos (ceros fuera de las semillas)
    tele_full = graph.nodes.leftOuterJoin(tele, P).mapValues(
        lambda v: v[1] if v[1] is not None else np.zeros(T, dtype=dtype)
    ).persist(LEVEL)

    ranks = tele_full  # arranque: cada tópico parte de su distribución de teletransporte
    zeros = np.zeros(T, dtype=dtype)
    history = []
    t0 = time.time()

    for it in range(1, max_iter + 1):
        sums = graph.links.join(ranks, P).flatMap(_spread).reduceByKey(add, P)
        dangling = ranks.join(graph.dangling, P).map(lambda kv: kv[1][0]).fold(zeros, add)
        tele_mass = (1.0 - beta) + beta * dangling  # vector de dimensión T

        new = tele_full.leftOuterJoin(sums, P).mapValues(
            lambda v: tele_mass * v[0] + beta * (v[1] if v[1] is not None else 0.0)
        ).persist(LEVEL)
        if checkpoint_every and it % checkpoint_every == 0:
            new.checkpoint()

        l1 = ranks.join(new, P).map(lambda kv: np.abs(kv[1][0] - kv[1][1])).fold(zeros, add)
        delta = float(np.max(l1))
        if ranks is not tele_full:
            ranks.unpersist()
        ranks = new
        history.append(delta)
        if verbose:
            print(f"[TSPR] iter {it:3d}  max L1={delta:.3e}  ({time.time() - t0:.0f}s)")
        if delta < tol:
            break
    return ranks, topics_used, history


def combine_topics(ranks, topics_used, weights):
    """Combinación en la consulta: r = sum_t w_t * r_t.

    weights: dict {tópico: peso}. Se normalizan para que sumen 1 (los tópicos no usados valen 0).
    """
    w = np.array([weights.get(t, 0.0) for t in topics_used], dtype=float)
    if w.sum() <= 0:
        raise ValueError("Los pesos deben sumar algo positivo")
    w = w / w.sum()
    return ranks.mapValues(lambda r: float(np.dot(w, r)))
