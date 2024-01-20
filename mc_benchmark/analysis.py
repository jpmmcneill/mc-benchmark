import pathlib

import duckdb

from mc_benchmark import benchmark_results_folder


def run_analysis(scenario: str):
    if (duckdb_path := benchmark_results_folder / f"{scenario}.duckdb").exists():
        con = duckdb.connect(str(duckdb_path))
        file_path = None
    elif (file_path := benchmark_results_folder / f"{scenario}.parquet").exists() or (file_path := benchmark_results_folder / f"{scenario}.csv").exists():
        con = duckdb.connect()
    else:
        raise FileNotFoundError(f"No benchmark output found for {scenario}.")

    time_analysis = con.execute(
        """
        with time_unnest as (
            select
                type,
                function,
                num_samples,
                function_arguments.roulette_sim.turn_limit,
                total_time,
                unnest(result.time_result) as time_taken
            from results
            where scenario_type = 'time'
        )

        select
            type,
            function,
            num_samples,
            turn_limit,
            total_time,
            avg(time_taken) as average_time_taken,
            var_samp(time_taken) as average_time_error
        from time_unnest
        group by all
        order by
            num_samples desc,
            turn_limit desc,
            average_time_taken asc
        """
    ).df()

    memory_analysis = con.execute(
        """
        with initial_unnest as (
            select
                type,
                function,
                num_samples,
                function_arguments.roulette_sim.turn_limit,
                total_time,
                unnest(result.memory_result) as memory_data
            from results
            where scenario_type = 'memory'
        ),

        memory_measurement_unnest as (
            select
                type,
                function,
                num_samples,
                turn_limit,
                total_time,
                memory_data.function_iteration,
                unnest(memory_data.data) as memory_taken,
            from initial_unnest
        )

        select
            type,
            function,
            num_samples,
            turn_limit,
            total_time,
            avg(memory_taken) as average_memory_taken,
            median(memory_taken) as median_memory_taken,
            max(memory_taken) as max_memory_taken,
        from memory_measurement_unnest
        group by all
        order by
            num_samples desc,
            turn_limit desc
        """
    ).df()

    import pdb
    pdb.set_trace()
