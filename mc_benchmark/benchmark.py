import enum
import pathlib
import time
import typing
import multiprocessing

import yaml
import pandas

from memory_profiler import memory_usage

from mc_benchmark.calculators import NumpyCalculator, DuckDBCalculator, NumbaCalculator


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
        min_runs = 3
        min_time = 10  # seconds
        max_runs = 1000
        results = []
        start = time.perf_counter()
        result = memory_usage((function, tuple(), arguments), **config)
        end = time.perf_counter()
        results.append(result)

        projected_min_runs = min_time / (end - start)
        for _ in range(
            min(max_runs, max(min_runs - 1, round(projected_min_runs)))
        ):
            result = memory_usage((function, tuple(), arguments), **config)
            results.append(result)
        return results

    @staticmethod
    def time_profiler(function, arguments, config={}):
        min_runs = 3
        min_time = 10  # seconds
        max_runs = 1000
        results = []
        start = time.perf_counter()
        function(**arguments)
        end = time.perf_counter()
        results.append(end-start)

        projected_min_runs = min_time / (end - start)
        for _ in range(
            min(max_runs, max(min_runs - 1, round(projected_min_runs)))
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
    def from_yaml(cls, name = "scenarios.yml"):
        file = pathlib.Path(__file__).parent.parent / name
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
        return [cls.parse_dict(d) for d in dicts]
    
    @classmethod
    def parse_dict(cls, data: dict):
        if data["type"] == "duckdb":
            calc = DuckDBCalculator
        elif data["type"] == "numpy":
            calc = NumpyCalculator
        elif data["type"] == "numba":
            calc = NumbaCalculator
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
        ) -> None:

        self.scenarios = scenarios
        self.queue: multiprocessing.Queue = multiprocessing.Queue()
        self.results_cache = {
            "type": [],
            "function": [],
            "run_iter": [],
            "total_time": [],
            "function_arguments": [],
            "scenario_type": [],
            "result": [],
            "profiler_arguments": [],
        }


    def process_scenarios(self):
        print("Starting benchmarking!")
        for scenario in self.scenarios:
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
                "type": scenario.type,
                "function": scenario.function.__name__,
                "run_iter": 1,
                "total_time": exec_time,
                "function_arguments": scenario.function_arguments,
                "scenario_type": scenario.scenario_type.value,
                "result": result,
                "profiler_arguments": scenario.profiler_arguments,
            }
            for k in self.results_cache:
                self.results_cache[k].append(result_data[k])
        print("Benchmarking done!")
        benchmark_result = pandas.DataFrame(self.results_cache)
        benchmark_result.to_csv("result.csv")
        import pdb
        pdb.set_trace()
        return None

    @staticmethod
    def check_files():
        folder = pathlib.Path(__file__).parent / "results"
        files = list(folder.glob("*.csv"))
        if not files:
            return 1


    def memory_benchmark():
        pass

    def time_benchmark():
        pass
