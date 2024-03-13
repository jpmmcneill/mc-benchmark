import duckdb

from mc_benchmark import benchmark_results_folder, analysis_folder


def run_analysis(scenario: str):
    if (duckdb_path := benchmark_results_folder / f"{scenario}.duckdb").exists():
        con = duckdb.connect(str(duckdb_path))
        file_path = None
    elif (file_path := benchmark_results_folder / f"{scenario}.parquet").exists() or (file_path := benchmark_results_folder / f"{scenario}.csv").exists():
        con = duckdb.connect()
    else:
        raise FileNotFoundError(f"No benchmark output found for {scenario}.")

    time_analysis = con.execute(
        f"""
        copy (
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
        ) to '{analysis_folder}/{scenario}_time.csv'
        """
    )

    memory_analysis = con.execute(
        f"""
        copy (
        with initial_unnest as (
            select
                scenario_number,
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
                scenario_number,
                type,
                function,
                num_samples,
                turn_limit,
                total_time,
                memory_data.function_iteration,
                unnest(memory_data.data) as memory_taken,
            from initial_unnest
        ),

        include_row_number as (
            select
                scenario_number,
                type,
                function,
                num_samples,
                turn_limit,
                total_time,
                function_iteration,
                memory_taken,
                0.1 * row_number() over (partition by scenario_number, function_iteration) as time_s
            from memory_measurement_unnest
        )

        select * from include_row_number
        ) to '{analysis_folder}/{scenario}_memory.csv'
        """
    )
