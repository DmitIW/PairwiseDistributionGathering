import numpy as np
import networkx as nx
import community as community_louvain

from typing import Generator, Any, Dict, Iterator

from data_gathering.base.time_utility import current_unix_minute


def get_partition(graph: nx.Graph, delete: bool) -> Dict[str, Any]:
    result = community_louvain.best_partition(graph)
    if delete:
        del graph
    return result


def get_graph(nodes: np.ndarray, delete: bool) -> nx.Graph:
    graph = nx.Graph()
    if nodes.shape[0] == 0:
        return graph

    if nodes.shape[1] != 3:
        raise TypeError("Unexpected the number of dimensions for nodes")
    graph.add_weighted_edges_from(nodes)
    if delete:
        del nodes
    return graph


def gathering_nodes(data_flow: Iterator) -> np.ndarray:
    return np.array([data for data in filter(lambda x: x is not None, data_flow)])


def get_current_slice() -> int:
    return current_unix_minute()


def algorithm(data_flow: Iterator) -> Generator[Any, Any, None]:
    slice_number = current_unix_minute()
    for node, comm in get_partition(get_graph(gathering_nodes(data_flow), True), True).items():
        yield int(node), int(comm), int(slice_number)
