import numpy as np

from mc_benchmark.calculators.base_calculator import BaseCalculator

class NumpyCalculator(BaseCalculator):

    
    @staticmethod
    def pi_calculator(num_samples: int = 1000):
        # interestingly, having seperate x and y makes this 30-40% faster
        x=np.random.random(size=num_samples)
        y=np.random.random(size=num_samples)
        d = x**2 + y**2 < 1
        r = 4 * np.count_nonzero(d) / d.size
        return r

    @staticmethod
    def casino_simulation(
        num_samples: int = 1000,
        turn_limit: int = 1000,
        starting_value: int = 1000,
        win_loss_diff: int = 10,
    ):
        # Initialize values array
        values = np.full((num_samples, turn_limit), starting_value, dtype=np.int32)

        for turn in range(1, turn_limit):
            # Generate random outcomes for each sample
            outcomes = np.random.randint(0, 37, size=num_samples)
            change = np.where(outcomes >= 19, win_loss_diff, -win_loss_diff)

            # Update values, but only for those samples not yet at 0
            values[:, turn] = np.where(
                values[:, turn - 1] > 0,
                values[:, turn - 1] + change,
                values[:, turn - 1]
            )

        sample_numbers = np.repeat(np.arange(1, num_samples + 1), turn_limit)
        turn_numbers = np.tile(np.arange(turn_limit), num_samples)
        flattened_values = values.flatten()
        return np.column_stack((sample_numbers, turn_numbers, flattened_values))
            
    @staticmethod
    def casino_simulation_aggregated(
        num_samples: int = 1000,
        turn_limit: int = 1000,
        starting_value: int = 1000,
        win_loss_diff: int = 10,
    ):
        # Initialize values array
        values = np.full((num_samples, turn_limit), starting_value, dtype=np.int32)

        for turn in range(1, turn_limit):
            # Generate random outcomes for each sample
            outcomes = np.random.randint(0, 37, size=num_samples)
            change = np.where(outcomes >= 19, win_loss_diff, -win_loss_diff)

            # Update values, but only for those samples not yet at 0
            values[:, turn] = np.where(
                values[:, turn - 1] > 0,
                values[:, turn - 1] + change,
                values[:, turn - 1]
            )

        # Calculate average value and SEM for each turn
        avg_values = values.mean(axis=0)
        sample = values.size / turn_limit
        std_dev = values.std(axis=0, ddof=1)
        sem = std_dev / np.sqrt(num_samples)

        return np.column_stack((np.arange(turn_limit), avg_values, np.full(turn_limit, sample), sem))

    @staticmethod
    def casino_simulation_vectorized(
        num_samples=1,
        turn_limit=1000,
        starting_value=1000,
        win_loss_diff=10
    ):
        # Generate all random outcomes at once
        outcomes = np.random.randint(0, 37, size=(num_samples, turn_limit))
        change = np.where(outcomes >= 19, win_loss_diff, -win_loss_diff)

        # Calculate cumulative sum of changes
        cumulative_changes = np.cumsum(change, axis=1)

        # Apply the starting value
        values = starting_value + cumulative_changes

        # Mask values after they reach zero using cumulative maximum
        zeroed_out_mask = np.maximum.accumulate(values <= 0, axis=1)
        values[zeroed_out_mask] = 0

        # Insert starting value as the first column
        values = np.hstack([np.full((num_samples, 1), starting_value), values[:, :-1]])

        sample_numbers = np.repeat(np.arange(1, num_samples + 1), turn_limit)
        turn_numbers = np.tile(np.arange(turn_limit), num_samples)
        flattened_values = values.flatten()
        return np.column_stack((sample_numbers, turn_numbers, flattened_values))

    @staticmethod
    def casino_simulation_vectorized_aggregated(
        num_samples=1,
        turn_limit=1000,
        starting_value=1000,
        win_loss_diff=10
    ):
        # Generate all random outcomes at once
        outcomes = np.random.randint(0, 37, size=(num_samples, turn_limit))
        change = np.where(outcomes >= 19, win_loss_diff, -win_loss_diff)

        # Calculate cumulative sum of changes
        cumulative_changes = np.cumsum(change, axis=1)

        # Apply the starting value
        values = starting_value + cumulative_changes

        # Mask values after they reach zero using cumulative maximum
        zeroed_out_mask = np.maximum.accumulate(values <= 0, axis=1)
        values[zeroed_out_mask] = 0

        # Insert starting value as the first column
        values = np.hstack([np.full((num_samples, 1), starting_value), values[:, :-1]])

        # sample_numbers = np.repeat(np.arange(1, num_samples + 1), turn_limit)
        # turn_numbers = np.tile(np.arange(turn_limit), num_samples)
        # flattened_values = values.flatten()
        # return np.column_stack((sample_numbers, turn_numbers, flattened_values))

        # Calculate average value and SEM for each turn
        avg_values = values.mean(axis=0)
        sample = values.size / turn_limit
        std_dev = values.std(axis=0, ddof=1)
        sem = std_dev / np.sqrt(num_samples)

        return np.column_stack((np.arange(turn_limit), avg_values, np.full(turn_limit, sample), sem))
