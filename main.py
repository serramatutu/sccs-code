import argparse
from collections import deque
import json
import logging
from typing import List, Dict, Tuple, Deque, Set
import sys

from simulator.workload import create_workload
from simulator.types import Transaction, TransactionType
from simulator.db import BasePartition, EagerPartition, DeferredPartition, ReferencePartition

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("main")

TIME_STEP = 0.001

def parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--out-file", required=True, type=str)
    parser.add_argument("--num-experiments", required=False, type=int, default=1)
    parser.add_argument("--num-partitions", required=True, type=int)
    parser.add_argument("--keyspace-size", required=True, type=int)
    parser.add_argument("--duration", required=True, type=float)
    parser.add_argument("--tps", required=True, type=float)
    parser.add_argument("--execution-time-avg", required=True, type=float)
    parser.add_argument("--max-defer-time", required=False, type=float, default=-1)

    return parser.parse_args(args)

def run_on_partitions(duration: float, workload: List[Transaction], partitions: List[BasePartition]) -> Tuple[Dict[int, int], Dict[Transaction, float]]:
    transaction_queue: Deque[Transaction] = deque(workload)

    time = 0
    while time < duration:
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

    read_values: Dict[Transaction, int] = {}
    latencies: Dict[Transaction, float] = {}
    for p in partitions:
        for transaction, latency in p.get_latencies().items():
            latencies[transaction] = latency

        for transaction, read_value in p.get_read_values().items():
            read_values[transaction] = read_value

    return read_values, latencies

def run(
    num_partitions: int,
    duration: float,
    max_defer_time: float,
    **kwargs
):
    workload = create_workload(
        **kwargs
    )

    reference: List[BasePartition] = [
        ReferencePartition(id=i) for i in range(num_partitions)
    ]

    eager: List[BasePartition] = [
        EagerPartition(id=i) for i in range(num_partitions)
    ]

    deferred: List[BasePartition] = [
        DeferredPartition(id=i, max_defer_time=max_defer_time) for i in range(num_partitions)
    ]

    reference_reads, _ = run_on_partitions(duration, workload, reference)
    eager_reads, eager_latencies = run_on_partitions(duration, workload, eager)
    deferred_reads, deferred_latencies = run_on_partitions(duration, workload, deferred)

    return workload, reference_reads, eager_reads, deferred_reads, eager_latencies, deferred_latencies

def main(args_list: List[str]):
    args = parse_args(args_list)

    results = []

    for _ in range(args.num_experiments):
        workload, reference_reads, eager_reads, deferred_reads, eager_lat, deferred_lat = run(
            max_defer_time=args.max_defer_time,
            num_partitions=args.num_partitions,
            tps_average=args.tps, 
            keyspace_size=args.keyspace_size, 
            duration=args.duration, 
            execution_average=args.execution_time_avg,
        )

        experiment_results = [
            {
                "id": t.id,
                "type": t.type.name,
                "key": t.key,
                "submit_time": t.submit_time,
                "execution_time": t.execution_time,
                "read_value": {
                    "reference": reference_reads.get(t),
                    "eager": eager_reads.get(t),
                    "deferred": deferred_reads.get(t)
                },
                "latency": {
                    "eager": eager_lat[t],
                    "deferred": deferred_lat[t]
                }
            }
            for t in workload
        ]
        results.append(experiment_results)

    experiment = {
        "parameters": {
            "num_partitions": args.num_partitions,
            "keyspace_size": args.keyspace_size,
            "tps_average": args.tps,
            "duration": args.duration,
            "execution_time_average": args.execution_time_avg,
            "time_resolution": TIME_STEP
        },
        "results": results,
    }

    with open(args.out_file, "w") as f:
        f.write(json.dumps(experiment, indent=4))


if __name__ == "__main__":
    main(sys.argv[1:])
