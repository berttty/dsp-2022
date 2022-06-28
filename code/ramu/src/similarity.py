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


if __name__ == '__main__':
