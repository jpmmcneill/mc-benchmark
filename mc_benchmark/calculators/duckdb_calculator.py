import concurrent.futures

import duckdb

from mc_benchmark.calculators.base_calculator import BaseCalculator

class DuckDBCalculator(BaseCalculator):
    @staticmethod
    def pi_calculator(num_samples: int = 1000, num_threads: int = 1):
        if num_threads == 1:
            data = duckdb.execute(
                f"""
                select 4 * sum(cast(random()**2 + random()**2 < 1 as int)) / count(*) as pi
                from generate_series(1, {num_samples})
                """
            ).arrow()
        else:
            def _run_query(_num_samples: int):
                con = duckdb.connect()
                return con.sql(
                    f"""
                    select sum(cast(random()**2 + random()**2 < 1 as int)) as num_in_circle, count(*) as num_total
                    from generate_series(1, {_num_samples})
                    """
                ).arrow()

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Using a dictionary comprehension to map future to its sequence number
                future_to_number = {
                    executor.submit(_run_query, num_samples // num_threads): number
                    for number in range(1, num_threads + 1)
                }

                dict_data = {}
            
                for future in concurrent.futures.as_completed(future_to_number):
                    number = future_to_number[future]
                    dict_data[number] = future.result()

            for i in dict_data.keys():
                exec(f"result_{i} = dict_data[{i}]")
            
            union_query = " union all ".join(f"select * from result_{i}" for i in dict_data)

            data = duckdb.sql(
                f"""
                select 4 * sum(num_in_circle) / sum(num_total) from ({union_query})
                """
            ).arrow()
        return data[0][0].as_py()


    @staticmethod
    def casino_simulation(num_samples: int = 1000, turn_limit: int = 1000, starting_value: int = 1000, win_loss_diff: int = 10):
        data = duckdb.execute(
            f"""
            with recursive casino as (
                select
                    generate_series as sample,
                    {starting_value} as value,
                    0 as turn
                from generate_series(1, {num_samples})
                union all
                select
                    casino.sample,
                    case
                        when casino.value = 0 then 0
                    else casino.value + (case when floor(random() * (37)) >= 19 then {win_loss_diff} else -{win_loss_diff} end)
                    end as value,
                    casino.turn + 1 as turn
                from casino
                where value >= 0 and casino.turn < {turn_limit}
            )

            select * from casino
            """
        ).arrow()
        return data

    @staticmethod
    def casino_simulation_aggregated(num_samples: int = 1000, turn_limit: int = 1000, starting_value: int = 1000, win_loss_diff: int = 10):
        data = duckdb.execute(
            f"""
            with recursive casino as (
                select
                    generate_series as sample,
                    {starting_value} as value,
                    0 as turn
                from generate_series(1, {num_samples})
                union all
                select
                    casino.sample,
                    case
                        when casino.value = 0 then 0
                    else casino.value + (case when floor(random() * (37)) >= 19 then {win_loss_diff} else -{win_loss_diff} end)
                    end as value,
                    casino.turn + 1 as turn
                from casino
                where value >= 0 and casino.turn < {turn_limit}
            )

            select
                turn,
                avg(value) as avg_value,
                -- using the inbuilt stddev_pop is much slower, so do it by hand
                sqrt(
                    (sum(value*value)/count(*) - avg(value)**2)/count(*)
                ) as avg_value_error
            from casino
            group by all
            order by turn asc
            """
        ).arrow()
        return data

    @staticmethod
    def casino_simulation_window_function(num_samples: int = 1000, turn_limit: int = 1000, starting_value: int = 1000, win_loss_diff: int = 10):
        data = duckdb.execute(
            f"""
            with casino as (
                select
                    samples.generate_series as sample,
                    turns.generate_series as turn,
                    {starting_value} + sum(
                        case when floor(random() * (37)) >= 19 then {win_loss_diff} else -{win_loss_diff} end
                    ) over (
                        partition by sample order by turn
                    ) as value
                from generate_series(1, {num_samples}) as samples
                cross join generate_series(1, {turn_limit}) as turns
            )

            select
                turn,
                avg(value) as avg_value,
                var_samp(value) as var_samp_value
            from casino
            group by all
            order by turn asc
            """
        ).arrow()
        return data



# TODO: threaded version(s)
    
    # how can we make it so that aggregated version is summable easily on the way out
