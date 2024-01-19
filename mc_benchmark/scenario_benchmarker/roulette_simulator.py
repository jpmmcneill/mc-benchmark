import time
import multiprocessing

import pandas as pd

from memory_profiler import memory_usage

from mc_benchmark.calculators.numpy_calculator import NumpyCalculator
from mc_benchmark.calculators.numba_calculator import NumbaCalculator
from mc_benchmark.calculators.duckdb_calculator import DuckDBCalculator


def profiler(function, arguments):
    return memory_usage((function, tuple(), dict(**arguments)), interval=0.1)


def duckdb_benchmarker(q, arguments):
    q.put(profiler(DuckDBCalculator.casino_simulation, arguments))


def numpy_benchmarker(q, arguments):
    q.put(profiler(NumpyCalculator.casino_simulation, arguments))


def numba_benchmarker(q, arguments):
    q.put(profiler(NumbaCalculator.casino_simulation, arguments))


if __name__ == '__main__':
    # num_samples=100000,
    # turn_limit=5000,

    # Benchmarking Duckdb...
    # max memory usage: 16864.953125
    # time taken: 9.183035135269165


    # In this... duckdb goes out of core!
    # max memory usage was actually ~ 190 GB according to activity monitor
    # num_samples=1000000,
    # turn_limit=5000,

    # Benchmarking Duckdb...
    # max memory usage: 24808.84375
    # time taken: 156.63889503479004

    queue = multiprocessing.Queue()

    print(f"Benchmarking Duckdb...")
    p = multiprocessing.Process(
        target=duckdb_benchmarker,
        args=(queue, dict(
            num_samples=1000000,
            turn_limit=3000,
            )
        )
    )
    start = time.time()
    p.start()
    p.join()
    result = queue.get()
    end = time.time()
    print(f"max memory usage: {max(result)}")
    print(f"time taken: {end - start}")

    print("Sleeping...")
    time.sleep(5)

    # print(f"Benchmarking Numpy...")
    # p = multiprocessing.Process(
    #     target=duckdb_benchmarker,
    #     args=(queue, dict(
    #         num_samples=100000,
    #         turn_limit=3000,
    #         )
    #     )
    # )
    # start = time.time()
    # p.start()
    # p.join()
    # result = queue.get()
    # end = time.time()
    # print(f"max memory usage: {max(result)}")
    # print(f"time taken: {end - start}")

    print(f"Benchmarking Numba...")
    p = multiprocessing.Process(
        target=numba_benchmarker,
        args=(queue, dict(
            num_samples=1000000,
            turn_limit=3000,
            )
        )
    )
    start = time.time()
    p.start()
    p.join()
    result = queue.get()
    end = time.time()
    print(f"max memory usage: {max(result)}")
    print(f"time taken: {end - start}")
