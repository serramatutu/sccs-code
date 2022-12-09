import math
from typing import List, Optional

import numpy as np

from simulator.types import Transaction, TransactionType


def create_workload(
    seed: Optional[int] = None,
    duration: int = 20,
    get_percentage: float = 1/3,
    overwrite_percentage: float = 1/3,
    increase_percentage: float = 1/3,
    tps_average: float = 10.0,
    tps_deviation: float = 1.0,
    execution_average: float = 1.0,
    execution_deviation: float = 1.0,
    keyspace_size: int = 10
) -> List[Transaction]:
    """Generate a transaction workload."""

    if not math.isclose(get_percentage + overwrite_percentage + increase_percentage, 1.0):
        raise ValueError("Sum of percentages must equal 1.")

    rng = np.random.default_rng(seed)

    transactions: List[Transaction] = []

    current_id = 0

    for second in range(duration):
        second_transactions = int(rng.normal(tps_average, tps_deviation))
        if second_transactions <= 0:
            continue

        submit_times = second + rng.random(second_transactions)
        submit_times.sort()
        
        for i in range(second_transactions):
            submit_time = submit_times[i]
            execution_time = max(0, rng.normal(execution_average, execution_deviation))
            chance = rng.random()

            if chance < get_percentage:
                transaction_type = TransactionType.GET
            elif chance < get_percentage + overwrite_percentage:
                transaction_type = TransactionType.OVERWRITE
            else:
                transaction_type = TransactionType.INCREASE

            transaction = Transaction(
                id=current_id,
                type=transaction_type,
                submit_time=float(submit_time),
                execution_time=float(execution_time),
                key=int(rng.integers(0, keyspace_size))
            )

            transactions.append(transaction)

            current_id += 1

    transactions.sort(key=lambda t: t.submit_time)

    return transactions
    