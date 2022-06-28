import os
import tempfile
from dtaidistance import dtw, util_numpy
from dtaidistance import dtw_visualisation as dtwvis
import numpy as np
from dtaidistance import clustering


def distance_assym_matrix(s):
    w = len(s)
    mat = [[0 for x in range(w)] for y in range(w)]

    for x in range(0, len(s)):
        mat[x][x] = 0.
        for y in range(x+1, len(s)):
            d = dtw.distance_fast(np.array(s[x]), np.array(s[y]))
            mat[x][y] = d
            mat[y][x] = d

    return np.matrix(mat)


def test_comparison():
    raw_series = [
        [0., 0, 1, 2, 1, 0, 1, 0, 0],
        [0., 1, 2, 0, 0, 0, 0, 0, 0],
        [1., 2, 0, 0, 0, 0, 0, 1, 1],
        [0., 0, 1, 2, 1, 0, 1, 0, 0],
        [0., 1, 2, 0, 0, 0, 0, 0, 0],
        [1., 2, 0, 0, 0, 0, 0, 1, 1]]

    series = np.matrix(raw_series)

    ds = dtw.distance_matrix_fast(series)
    test = distance_assym_matrix(raw_series)

    print(ds)
    print(test)

    print((ds == test).all())


def regular():
    directory = os.getcwd()

    series = np.matrix([
        [0., 0, 1, 2, 1, 0, 1, 0, 0],
        [0., 1, 2, 0, 0, 0, 0, 0, 0],
        [1., 2, 0, 0, 0, 0, 0, 1, 1],
        [0., 0, 1, 2, 1, 0, 1, 0, 0],
        [0., 1, 2, 0, 0, 0, 0, 0, 0],
        [1., 2, 0, 0, 0, 0, 0, 1, 1]])
    ds = dtw.distance_matrix_fast(series)



    model1 = clustering.Hierarchical(dtw.distance_matrix_fast, {})
    model2 = clustering.HierarchicalTree(model1)
    #cluster_idx = model2.fit(series)
    # SciPy linkage clustering
    model3 = clustering.LinkageTree(dtw.distance_matrix_fast, {})
    cluster_idx = model3.fit(series)

    print(cluster_idx)

    if directory:
        hierarchy_fn = os.path.join(directory, "hierarchy.png")
        graphviz_fn = os.path.join(directory, "hierarchy.dot")
    else:
        file = tempfile.NamedTemporaryFile()
        hierarchy_fn = file.name + "_hierarchy.png"
        graphviz_fn = file.name + "_hierarchy.dot"

    if not dtwvis.test_without_visualization():
        model3.plot(hierarchy_fn)
        print("Figure saved to", hierarchy_fn)

    with open(graphviz_fn, "w") as ofile:
        print(model3.to_dot(), file=ofile)
    print("Dot saved to", graphviz_fn)


def new():
    np = util_numpy.np
    directory = os.getcwd()

    raw_series = [
        [0., 0, 1, 2, 1, 0, 1, 0, 0, 5, 7, 1],
        [0., 1, 2, 0, 0, 0, 0, 0, 0, 2, 1, 0, 1, 0, 0, 5, 7, 1],
        [1., 2, 0, 0, 0, 0, 0, 1, 1],
        [0., 0, 1, 2, 1, 0, 1, 0, 0, 3, 1],
        [0., 1, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0],
        [1., 2, 0, 0, 0, 0, 1, 1, 1],
        [0., 0, 0, 0, 2, 1, 0, 1, 0, 0, 5, 7, 1],
        [0., 0, 1, 2, 1, 0]
    ]
        # [
        # [0., 0, 1, 2, 1, 0, 1, 0, 0],
        # [0., 1, 2, 0, 0, 0, 0, 0, 0],
        # [1., 2, 0, 0, 0, 0, 0, 1, 1],
        # [0., 0, 1, 2, 1, 0, 1, 0, 0],
        # [0., 1, 2, 0, 0, 0, 0, 0, 0],
        # [1., 2, 0, 0, 0, 0, 0, 1, 1]]


    model3 = clustering.LinkageTree(distance_assym_matrix, {})
    cluster_idx = model3.fit(raw_series)

    print(cluster_idx)

    if directory:
        hierarchy_fn = os.path.join(directory, "hierarchy.png")
        graphviz_fn = os.path.join(directory, "hierarchy.dot")
    else:
        file = tempfile.NamedTemporaryFile()
        hierarchy_fn = file.name + "_hierarchy.png"
        graphviz_fn = file.name + "_hierarchy.dot"

    # if not dtwvis.test_without_visualization():
    #     model3.plot(hierarchy_fn)
    #     print("Figure saved to", hierarchy_fn)

    with open(graphviz_fn, "w") as ofile:
        print(model3.to_dot(), file=ofile)
    print("Dot saved to", graphviz_fn)


if __name__ == '__main__':
    # regular()
    new()
