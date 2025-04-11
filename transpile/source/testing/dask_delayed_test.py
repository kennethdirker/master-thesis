import subprocess           # Execute shell command line 
import dask.delayed
import dask.delayed
import dask.delayed
import graphviz             # To visualize the graph
from pathlib import Path
# def main():
#     # a - b - c - d - e
#     a = dask.delayed(wrapper)(400)
#     b = dask.delayed(wrapper)(a)
#     c = dask.delayed(wrapper)(b)
#     d = dask.delayed(wrapper)(c)
#     e = dask.delayed(wrapper)(d)
#     path = "graph.svg"
#     e.visualize(filename=path)
#     res = e.compute()
#     print(res)
#     print("finished.")



def wrapper(word, *chaining_element):
    return subprocess.run(["echo", f"Hello, {word}"])

class Node:
    def __init__(self, parents, value):
        self.parents = parents
        self.value = value

    def __str__(self):
        return str(self.value)

def create_nodes(elems):
    nodes = {}
    out_edges = {}

    parents = []
    for i in elems:
        for j in i:
            nodes[j] = Node(parents, j)
            for p in parents:
                if p in out_edges:
                    out_edges[p].append(j)
                else: 
                    out_edges[p] = [j]
        parents = i

    print(nodes)
    print(out_edges)
    return nodes, out_edges


def create_graph(nodes: dict[str: Node], out_edges: dict[str, list[str]]):
    d = None
    for node in nodes:
        d = dask.delayed(wrapper)(node.value, *node.parents)
    return d


def main():
    progs = [["A.", "B."], ["C."], ["D.", "E."], ["F."]]
    nodes, out_edges = create_nodes(progs)
    key_map = {}

    d = create_graph(nodes, out_edges)
    # for elem in progs:
    #     if isinstance(elem, list):
    #         d = dask.delayed(wrapper)(w, *[d, *elem])
    path = "graph.svg"
    d.visualize(filename=path)
    d.compute()

if __name__ == "__main__":
    main()