import time
import multiprocessing

import pandas as pd

from memory_profiler import memory_usage

from mc_benchmark.calculators.numpy_calculator import NumpyCalculator
from mc_benchmark.calculators.duckdb_calculator import DuckDBCalculator

sizes_to_sample = [
    1,
    10,
    30,
    60,
    100,
    300,
    600,
    1000,
    3000,
    6000,
    10000,
    30000,
    60000,
    100000,
    300000,
    600000,
    1000000,
    3000000,
    6000000,
    10000000,
    30000000,
    60000000,
    100000000,
    300000000,
    600000000,
    1000000000,
]

def profiler(function, arguments):
    return memory_usage((function, tuple(), dict(**arguments)), interval=0.05)
   
def numpy_benchmarker(q, arguments):
    q.put(profiler(NumpyCalculator.pi_calculator, arguments))

def duckdb_benchmarker(q, arguments):
    q.put(profiler(DuckDBCalculator.pi_calculator, arguments))

if __name__ == '__main__':
    results = {
        "type": [],
        "num_samples": [],
        "time": [],
        "max_memory": [],
        "memory": []
    }
    # use multiprocessing to avoid memory leaking in numpy.
    # if a naive loop is lost, numpy only collects garbage when all memory is used.
    # (at least according to memory_usage)
    queue = multiprocessing.Queue()
    for samples in sizes_to_sample:
        print(f"Benchmarking Numpy for {samples} points...")
        p = multiprocessing.Process(
            target=numpy_benchmarker,
            args=(queue, dict(
                num_samples=samples
                )
            )
        )
        start = time.time()
        p.start()
        p.join() # this blocks until the process terminates
        result = queue.get()
        end = time.time()
        print(f"max memory usage: {max(result)}")
        print(f"time taken: {end - start}")
        results["type"].append("numpy")
        results["num_samples"].append(samples)
        results["time"].append(end - start)
        results["max_memory"].append(max(result))
        results["memory"].append(result)

        time.sleep(5)

        print(f"Benchmarking Duckdb for {samples} points...")
        p = multiprocessing.Process(
            target=duckdb_benchmarker,
            args=(queue, dict(
                num_samples=samples
                )
            )
        )
        start = time.time()
        p.start()
        p.join() # this blocks until the process terminates
        result = queue.get()
        end = time.time()
        print(f"max memory usage: {max(result)}")
        print(f"time taken: {end - start}")
        results["type"].append("duckdb")
        results["num_samples"].append(samples)
        results["time"].append(end - start)
        results["max_memory"].append(max(result))
        results["memory"].append(result)

    df = pd.DataFrame(results)
    df.to_csv("pi_benchmark.csv")
    import pdb
    pdb.set_trace()

