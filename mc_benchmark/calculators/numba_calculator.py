import random
import math

import numba as nb
import numpy as np

from mc_benchmark.calculators.base_calculator import BaseCalculator

class NumbaCalculator(BaseCalculator):
    
    @staticmethod
    @nb.jit(nopython=True)
    def pi_calculator(num_samples: int = 1000):
        return 4 * sum(
            [
                1 if np.random.random()**2 + np.random.random()**2 < 1
                else 0
                for _ in range(num_samples)
            ]
        ) / num_samples

    @staticmethod
    @nb.jit(nopython=True)
    def casino_simulation(
        num_samples: int = 1000,
        turn_limit: int = 1000,
        starting_value: int = 1000,
        win_loss_diff: int = 10,
    ):
        # Initialize values array
        values = []
        for sample in range(0, num_samples):
            values.append([starting_value])
            for _ in range(1, turn_limit+1):
                # Generate random outcomes for each sample
                outcome = random.randint(0, 37)
                if values[sample][-1] > 0 and outcome >= 19:
                    values[sample].append(values[sample][-1] + 10)
                elif values[sample][-1] > 0 and outcome < 19:
                    values[sample].append(values[sample][-1] - 10)
                else:
                    values[sample].append(0)

        return values

    @staticmethod
    @nb.jit(nopython=True)
    def casino_simulation_aggregated(
        num_samples: int = 1000,
        turn_limit: int = 1000,
        starting_value: int = 1000,
        win_loss_diff: int = 10,
    ):
        # Initialize values array
        values = []
        for sample in range(0, num_samples):
            values.append([starting_value])
            for _ in range(1, turn_limit+1):
                # Generate random outcomes for each sample
                outcome = random.randint(0, 37)
                if values[sample][-1] > 0 and outcome >= 19:
                    values[sample].append(values[sample][-1] + 10)
                elif values[sample][-1] > 0 and outcome < 19:
                    values[sample].append(values[sample][-1] - 10)
                else:
                    values[sample].append(0)

        turns = []
        averages = []
        for col_index in range(len(values[0])):
            averages.append(
                sum([sample[col_index] for sample in values]) / len(values)
            )
            turns.append(col_index)

        sem = []
        for col_index in range(len(values[0])):
            sem.append(
                math.sqrt(
                    (
                        sum([sample[col_index]**2 for sample in values]) / len(values)
                        - averages[col_index]**2
                    ) / len(values[0])
                )
            )

        return turns, averages, sem
