import numpy as np
import polars as pl

from mc_benchmark.calculators.base_calculator import BaseCalculator

class PolarsCalculator(BaseCalculator):

    @staticmethod
    def pi_calculator(num_samples: int = 1000):
        data = {"x": np.random.uniform(0, 1, num_samples), "y": np.random.uniform(0, 1, num_samples)}
        lf = pl.LazyFrame(data)
        lf = lf.with_columns((pl.col("x") ** 2 + pl.col("y")**2 < 1).alias("in_circle"))
        ratio = lf.with_columns("in_circle").select([
            (4 * pl.col('in_circle').sum() / pl.col('in_circle').count()).alias('pi')
        ]).collect()
        return ratio['pi'][0]

    @staticmethod
    def casino_simulation_base(
        num_samples: int = 1000,
        turn_limit: int = 1000,
        starting_value: int = 1000,
        win_loss_diff: int = 10,
        aggregate: bool = True
    ):
        lf_samples = pl.LazyFrame({"sample": np.arange(1, num_samples+1)})
        lf_turns = pl.LazyFrame({"turn": np.arange(1, turn_limit+1)})  # TODO include turn 0

        lf_joined = lf_samples.join(lf_turns, how="cross")

        lf_joined = lf_joined.with_columns(
            pl.Series("roll", np.random.randint(0, 37, size=(turn_limit)*num_samples))
        )

        lf_joined = lf_joined.with_columns(
            pl.lit(starting_value).alias('value'),
            pl.when(pl.col('roll')>=19)
            .then(win_loss_diff)
            .otherwise(-win_loss_diff)
            .alias('value_diff')
        )

        # ERROR WITH ONE OF THESE TYPES
        lf_zero_turn = lf_samples.with_columns(
            pl.lit(0).cast(pl.Int64).alias('turn'),
            pl.lit(0).cast(pl.Int64).alias('roll'),
            pl.lit(starting_value).cast(pl.Int32).alias('value'),
            pl.lit(0).cast(pl.Int32).alias('value_diff')
        )
        
        lf_joined = pl.concat([lf_zero_turn, lf_joined])

        lf_joined = lf_joined.select(
            pl.col(["sample", "turn", "value", "value_diff"]),
            pl.col("value_diff").cum_sum().over(["sample"]).alias("cum_value_diff"),
        )

        # Identify the first turn where cum_value_diff reaches -starting_value for each sample
        mask = (lf_joined.filter(pl.col("cum_value_diff") == -starting_value)
                .group_by("sample")
                .agg(pl.min("turn").alias("out_of_money_turn")))
        lf_joined = lf_joined.join(mask, on=["sample"], how="left")

        lf_joined = lf_joined.with_columns(
            pl.when(pl.col("turn") >= pl.col("out_of_money_turn"))
            .then(-1000)
            .otherwise(pl.col("cum_value_diff"))
            .alias("cum_value_diff_adjusted")
        ).with_columns(
            (pl.col("value") + pl.col("cum_value_diff_adjusted")).alias("value")
        )

        resolved = lf_joined.collect()[["sample", "turn", "value"]]

        if aggregate:
            resolved = resolved.group_by("turn").agg([
                pl.col("value").mean().alias("avg_value"),
                (pl.col("value").std() / (pl.count("value")**0.5)).alias("avg_value_error")
            ]).sort("turn")
        return resolved

    @staticmethod
    def casino_simulation(
        num_samples: int = 1000,
        turn_limit: int = 1000,
        starting_value: int = 1000,
        win_loss_diff: int = 10
    ):
        PolarsCalculator.casino_simulation_base(
            num_samples=num_samples,
            turn_limit=turn_limit,
            starting_value=starting_value,
            win_loss_diff=win_loss_diff,
            aggregate=False
        )

    @staticmethod
    def casino_simulation_aggregated(
        num_samples: int = 1000,
        turn_limit: int = 1000,
        starting_value: int = 1000,
        win_loss_diff: int = 10
    ):
        PolarsCalculator.casino_simulation_base(
            num_samples=num_samples,
            turn_limit=turn_limit,
            starting_value=starting_value,
            win_loss_diff=win_loss_diff,
            aggregate=True
        )
