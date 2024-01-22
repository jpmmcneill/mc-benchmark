import enum
import pathlib
import time
import typing
import multiprocessing

import yaml
import pandas
import duckdb

from memory_profiler import memory_usage

from mc_benchmark import benchmark_results_folder, scenario_folder
from mc_benchmark.calculators import NumpyCalculator, DuckDBCalculator, NumbaCalculator, PolarsCalculator

MIN_RUNS_PER_PROFILE = 5
MIN_TIME_PER_PROFILE = 10  # seconds
MAX_RUNS_PER_PROFILE = 1000

class ScenarioType(str, enum.Enum):
    NONE = "none"
    MEMORY = "memory"
    TIME = "time"

    @property
    def profiler(self):
        mappings = {
            ScenarioType.NONE: ScenarioType.no_profiler,
            ScenarioType.MEMORY: ScenarioType.memory_profiler,
            ScenarioType.TIME: ScenarioType.time_profiler,
        }
        return mappings[self]

    @staticmethod
    def no_profiler(function, arguments, config={}):
        return function(**arguments)

    @staticmethod
    def memory_profiler(function, arguments, config={}):
        results = []
        start = time.perf_counter()
        result = memory_usage((function, tuple(), arguments), **config)
        end = time.perf_counter()
        results.append({"function_iteration": 1, "data": result})

        projected_min_runs = MIN_TIME_PER_PROFILE / (end - start)
        for i in range(
            min(MAX_RUNS_PER_PROFILE, max(MIN_RUNS_PER_PROFILE - 1, round(projected_min_runs)))
        ):
            result = {"function_iteration": i+2, "data": memory_usage((function, tuple(), arguments), **config)}
            results.append(result)
        return results

    @staticmethod
    def time_profiler(function, arguments, config={}):
        results = []
        start = time.perf_counter()
        function(**arguments)
        end = time.perf_counter()
        results.append(end-start)

        projected_min_runs = MIN_TIME_PER_PROFILE / (end - start)
        for _ in range(
            min(MAX_RUNS_PER_PROFILE, max(MIN_RUNS_PER_PROFILE - 1, round(projected_min_runs)))
        ):
            start = time.perf_counter()
            function(**arguments)
            end = time.perf_counter()
            results.append(end-start)
        return results


def scenario_wrapper(function, function_arguments, queue, profiler, profiler_arguments):
    start_time = time.time()
    if not profiler:
        result = function(**function_arguments)
    else:
        result = profiler(function, function_arguments)
    end_time = time.time()
    queue.put((result, end_time - start_time))


class Scenario:
    def __init__(
            self,
            type: str,
            function: typing.Callable,
            function_arguments: dict,
            scenario_type: str,
            profiler_arguments: dict = {},
        ) -> None:
        self.type = type
        self.function = function
        self.function_arguments = function_arguments
        self.scenario_type = ScenarioType(scenario_type)
        self.profiler = self.scenario_type.profiler
        self.profiler_arguments = profiler_arguments

    def to_process(self, queue):
        return multiprocessing.Process(
            target=scenario_wrapper,
            args=(self.function, self.function_arguments, queue, self.profiler, self.profiler_arguments)
        )
    
    def __repr__(self) -> str:
        return f"""
        - type = {self.type}
        - function = {self.function.__name__}
        - arguments = {self.function_arguments}
        - profiler = {self.scenario_type.value}
        """
    
    @classmethod
    def parse_dict(cls, data: dict):
        if data["type"] == "duckdb":
            calc = DuckDBCalculator
        elif data["type"] == "numpy":
            calc = NumpyCalculator
        elif data["type"] == "numba":
            calc = NumbaCalculator
        elif data["type"] == "polars":
            calc = PolarsCalculator
        function = getattr(calc, data["function"])
        return cls(
            type=data["type"],
            function=function,
            function_arguments=data.get("function_arguments", {}),
            scenario_type=data["scenario_type"],
            profiler_arguments=data.get("profiler_arguments", {}),
        )

class Benchmark:
    def __init__(
            self,
            scenarios: list[Scenario],
            name: str
        ) -> None:

        self.scenarios = scenarios
        self.name = name
        self.queue: multiprocessing.Queue = multiprocessing.Queue()

    def process_scenarios(self):
        print("Starting benchmarking!")
        con = duckdb.connect(str(benchmark_results_folder / f"{self.name}.duckdb"))
        con.sql("""
        create or replace table results (
            type text,
            scenario_number integer,
            scenario_type text,
            function text,
            num_samples integer,
            function_arguments union(
                -- pi_calc is always empty, this is what {} looks like in duckdb...
                pi_calc map(integer, integer),
                roulette_sim struct(turn_limit integer)
            ),
            profiler_arguments text,
            total_time double,
            result union(
                time_result double[],
                memory_result struct(function_iteration integer, data double[])[]
            )
        )
        """)
        for i, scenario in enumerate(self.scenarios):
            p = scenario.to_process(self.queue)
            print("Running scenario:")
            print(scenario)
            p.start()
            p.join()
            result, exec_time = self.queue.get()
            print(f"Finished scenario in {exec_time} seconds")
            print("Sleeping...")
            time.sleep(3)
            result_data = {
                "type": [scenario.type],
                "scenario_number": [i],
                "scenario_type": [scenario.scenario_type.value],
                "function": [scenario.function.__name__],
                "num_samples": [scenario.function_arguments.pop("num_samples")],
                "function_arguments": [scenario.function_arguments],
                "profiler_arguments": [scenario.profiler_arguments],
                "total_time": [exec_time],
                "result": [result],
            }
            scenario_result = pandas.DataFrame(result_data)
            con.execute("insert into results select * from scenario_result")
        print("Benchmarking done!")
        print("Outputting data...")
        con.execute(f"copy (select * from results) to '{benchmark_results_folder}/{self.name}.csv'")
        con.execute(f"copy (select * from results) to '{benchmark_results_folder}/{self.name}.parquet'")
        return

    def memory_benchmark():
        pass

    def time_benchmark():
        pass

    @classmethod
    def from_yaml(cls, scenario: str):
        file = scenario_folder / (scenario+".yml")
        with open(file) as f:
            data = yaml.safe_load(f)
        dicts = []
        for data_dict in data:
            if isinstance(samples:=data_dict.get("function_arguments", {}).get("num_samples"), list):
                dicts.extend([
                    {**data_dict, 'function_arguments': {**data_dict['function_arguments'], 'num_samples': num_sample}}
                    for num_sample in samples
                ])
            else:
                dicts.append(data_dict)
        scenarios = [Scenario.parse_dict(d) for d in dicts]
        return cls(scenarios, name=scenario)
