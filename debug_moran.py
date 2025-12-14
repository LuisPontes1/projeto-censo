import pandas as pd
import numpy as np
from libpysal.weights import W
from esda.moran import Moran_Local
import warnings

# Mock data
N = 100
k = 5
values = np.random.rand(N)
# Create random neighbors (indices)
neighbors_indices = np.random.randint(0, N, size=(N, k))

print("Testing W construction...")
neighbors_dict = {i: neighbors_indices[i].tolist() for i in range(N)}
w = W(neighbors_dict, silence_warnings=True)
w.transform = 'r'
print(f"W created. n={w.n}")

from joblib import Parallel, delayed

def worker(values, neighbors_indices):
    neighbors_dict = {i: neighbors_indices[i].tolist() for i in range(len(values))}
    w = W(neighbors_dict, silence_warnings=True)
    w.transform = 'r'
    lm = Moran_Local(values, w, permutations=99, n_jobs=1)
    return lm.Is[0]

print("Testing Parallel Moran_Local...")
try:
    results = Parallel(n_jobs=2)(
        delayed(worker)(values, neighbors_indices) for _ in range(4)
    )
    print("Parallel success!", results)
except Exception as e:
    print("Parallel failed:")
    print(e)

