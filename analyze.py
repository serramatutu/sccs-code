import json
import statistics as stat
import sys
from typing import List, Tuple

def get_latency_stats(experiments, name: str) -> Tuple[float, float]:
    latencies = [
        transaction["latency"][name]
        for experiment in experiments["results"]
        for transaction in experiment
    ]

    return stat.mean(latencies), stat.median(latencies)

def get_read_ratios(experiments, name: str) -> Tuple[int, int]:
    correct_reads = 0
    wrong_reads = 0

    for experiment in experiments["results"]:
        for transaction in experiment:
            if transaction["type"] != "GET":
                continue
            
            if transaction["read_value"]["reference"] == transaction["read_value"][name]:
                correct_reads += 1
            else:
                # if name == "deferred":
                #     print(transaction)
                wrong_reads += 1

    total_reads = correct_reads + wrong_reads

    return correct_reads/total_reads, wrong_reads/total_reads


def get_analysis(experiments, name: str):
    lat_avg, lat_med = get_latency_stats(experiments, name)

    correct_pct, wrong_pct = get_read_ratios(experiments, name)

    return {
        "latency": {
            "average": lat_avg,
            "median": lat_med,
        },
        "reads": {
            "correct": correct_pct,
            "lost_update": wrong_pct
        }
    }


def main(args: List[str]):
    experiments_file = args[0]
    with open(experiments_file, "r") as f:
        experiments = json.loads(f.read())

    def print_name(name: str):
        analysis = get_analysis(experiments, name)
        print(name)
        print(json.dumps(analysis, indent=4))

    print_name("eager")
    print_name("deferred")


if __name__ == "__main__":
    main(sys.argv[1:])
