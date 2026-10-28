"""Representación del grafo como RDDs particionados por hash (base de los tres algoritmos).

Idea MapReduce (Leskovec et al., cap. 5): la matriz de transición se guarda dispersa como lista de
adyacencia `(u, [v1, v2, ...])`. Esa lista se particiona UNA sola vez por hash de la clave (`u`) y se
cachea. Los vectores de rango también se particionan por hash con el mismo número de particiones,
así que `links.join(ranks)` es local a cada partición: lo único que viaja por la red en cada
iteración son las contribuciones `(v, r_u/deg(u))` que se suman en el reduce.
"""
from array import array

from pyspark import StorageLevel

LEVEL = StorageLevel.MEMORY_AND_DISK


def read_edges(spark, path):
    """parquet con columnas (src, dst) -> RDD[(src, dst)]"""
    return spark.read.parquet(str(path)).select("src", "dst").rdd.map(lambda r: (r[0], r[1]))


def read_node_ids(spark, path):
    """parquet con columna `id` -> RDD[id]"""
    return spark.read.parquet(str(path)).select("id").rdd.map(lambda r: r[0])


def build_adjacency(edges, num_partitions):
    """RDD[(u, v)] -> RDD[(u, array('q', destinos))], particionado por hash y cacheado.

    Se eliminan aristas repetidas, pero NO los auto-lazos (eso se decide en la ingesta).
    Se usa `array('q')` en vez de listas de Python para ocupar ~8 bytes por arista.
    """
    return (
        edges.groupByKey(num_partitions)
        .mapValues(lambda dsts: array("q", sorted(set(dsts))))
        .persist(LEVEL)
    )


def transpose(links, num_partitions):
    """Grafo transpuesto: (v, array de u tales que u -> v). Lo usa HITS."""
    return (
        links.flatMap(lambda kv: ((v, kv[0]) for v in kv[1]))
        .groupByKey(num_partitions)
        .mapValues(lambda srcs: array("q", srcs))
        .persist(LEVEL)
    )


def keyed_nodes(edges, nodes, num_partitions):
    """RDD[(n, None)] con todos los nodos (extremos de aristas + `nodes` si se da), particionado por hash."""
    ends = edges.flatMap(lambda e: ((e[0], None), (e[1], None)))
    if nodes is not None:
        ends = ends.union(nodes.map(lambda n: (n, None)))
    return ends.reduceByKey(lambda a, b: None, num_partitions).persist(LEVEL)


class Graph:
    """Grafo dirigido listo para los algoritmos.

    - `links`    : RDD (u, array de destinos)           nodos con al menos una arista de salida
    - `rlinks`   : RDD (v, array de orígenes)           (se calcula la primera vez que se pide)
    - `nodes`    : RDD (n, None)                        todos los nodos
    - `dangling` : RDD (n, None)                        nodos sin aristas de salida
    - `N`        : número de nodos
    """

    def __init__(self, edges, nodes=None, num_partitions=64):
        self.P = num_partitions
        self.links = build_adjacency(edges, num_partitions)
        self.nodes = keyed_nodes(edges, nodes, num_partitions)
        self.N = self.nodes.count()
        self.dangling = self.nodes.subtractByKey(self.links, num_partitions).persist(LEVEL)
        self._rlinks = None

    @property
    def rlinks(self):
        if self._rlinks is None:
            self._rlinks = transpose(self.links, self.P)
        return self._rlinks

    def num_edges(self):
        return self.links.map(lambda kv: len(kv[1])).sum()

    def num_dangling(self):
        return self.dangling.count()

    def unpersist(self):
        for rdd in (self.links, self._rlinks, self.nodes, self.dangling):
            if rdd is not None:
                rdd.unpersist()
