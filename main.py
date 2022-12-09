from collections import deque
import json
import logging
from typing import List, Dict, Tuple, Deque, Set

from simulator.workload import create_workload
from simulator.types import Transaction
from simulator.db import BasePartition, EagerPartition, DeferredPartition, ReferencePartition

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger("main")

TIME_STEP = 0.001

DURATION = 30
EXECUTION_TIME = 1

def run_on_partitions(workload: List[Transaction], partitions: List[BasePartition]) -> Tuple[Dict[int, int], Dict[Transaction, float]]:
    transaction_queue: Deque[Transaction] = deque(workload)

    time = 0
    while time < DURATION:
        for p in partitions:
            p.clock(time)

        while len(transaction_queue) > 0 and transaction_queue[0].submit_time <= time:
            transaction = transaction_queue.popleft()
            partition = partitions[transaction.key // len(partitions)]
            partition.submit_transaction(transaction)

        time += TIME_STEP

    unfinished_partitions: Set[BasePartition] = set(
        p for p in partitions if not p.is_finished
    )

    # wait for all partitions to finish
    while len(unfinished_partitions) > 0:
        finished_partitions = set()
        for p in unfinished_partitions:
            p.clock(time)
            if p.is_finished:
                finished_partitions.add(p)
        unfinished_partitions = unfinished_partitions.difference(finished_partitions)
        time += TIME_STEP

    logger.debug("FINISHED")
    for p in partitions:
        logger.debug(f"partition 0 -> {len(p._pending)} pending, {len(p._started)} started, {len(p._finished_at)} finished")
    logger.debug("-------------------------")

    # TODO: pythonize
    keys: Dict[int, int] = {}
    latencies: Dict[Transaction, float] = {}
    for p in partitions:
        for transaction, latency in p.get_latencies().items():
            latencies[transaction] = latency

        for key, value in p.keys.items():
            keys[key] = value

    return keys, latencies

def run():
    num_partitions = 10
    keyspace_size = 20
    tps_average = 10

    workload = create_workload(
        tps_average=tps_average, 
        keyspace_size=keyspace_size, 
        duration=DURATION, 
        execution_average=EXECUTION_TIME
    )

    reference: List[BasePartition] = [
        ReferencePartition(id=i) for i in range(num_partitions)
    ]

    eager: List[BasePartition] = [
        EagerPartition(id=i) for i in range(num_partitions)
    ]

    deferred: List[BasePartition] = [
        DeferredPartition(id=i) for i in range(num_partitions)
    ]

    reference_keys, _ = run_on_partitions(workload, reference)
    eager_keys, eager_latencies = run_on_partitions(workload, eager)
    deferred_keys, deferred_latencies = run_on_partitions(workload, deferred)

    return workload, reference_keys, eager_keys, deferred_keys, eager_latencies, deferred_latencies

def main():
    experiments = 1
    results = []
    for _ in range(experiments):
        workload, reference_keys, eager_keys, deferred_keys, eager_lat, deferred_lat = run()

        experiment_results = {
            "workload": [
                {
                    "id": t.id,
                    "type": t.type.name,
                    "key": t.key,
                    "submit_time": t.submit_time,
                    "execution_time": t.execution_time,
                    "latency": {
                        "eager": eager_lat[t],
                        "deferred": deferred_lat[t]
                    }
                }
                for t in workload
            ],
            "keys": {
                ref_key: {
                    "reference": ref_value,
                    "eager": eager_keys[ref_key],
                    "deferred": deferred_keys[ref_key]
                }
                for ref_key, ref_value in reference_keys.items()
            }
        }
        results.append(experiment_results)

    with open("results.json", "w") as f:
        f.write(json.dumps(results, indent=4))


if __name__ == "__main__":
    main()
