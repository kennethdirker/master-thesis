import subprocess           # Execute shell command line 
import dask.delayed
import graphviz             # To visualize the graph
from pathlib import Path



def wrapper(id):
    print(id)
    return id + 1
    




def main():
    # a - b - c - d - e
    a = dask.delayed(wrapper)(400)
    b = dask.delayed(wrapper)(a)
    c = dask.delayed(wrapper)(b)
    d = dask.delayed(wrapper)(c)
    e = dask.delayed(wrapper)(d)
    path = "graph.svg"
    e.visualize(filename=path)
    res = e.compute()
    print(res)
    print("finished.")


if __name__ == "__main__":
    main()