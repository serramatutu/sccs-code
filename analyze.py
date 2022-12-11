import json
import statistics as stat
import sys
import os
from typing import List, Tuple

import matplotlib.pyplot as plt

def get_latency_stats(experiments, name: str) -> Tuple[float, float]:
    latencies = [
        transaction["latency"][name]
        for experiment in experiments["results"]
        for transaction in experiment
        if transaction["type"] == "GET"
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


def get_analysis(experiments):
    eager_lat_avg, eager_lat_med = get_latency_stats(experiments, "eager")
    eager_correct_pct, eager_wrong_pct = get_read_ratios(experiments, "eager")

    deferred_lat_avg, deferred_lat_med = get_latency_stats(experiments, "deferred")
    deferred_correct_pct, deferred_wrong_pct = get_read_ratios(experiments, "deferred")

    return {
        "parameters": experiments["parameters"],
        "eager": {
            "latency": {
                "average": eager_lat_avg,
                "median": eager_lat_med,
            },
            "reads": {
                "correct": eager_correct_pct,
                "lost_update": eager_wrong_pct
            }
        },
        "deferred": {
            "latency": {
                "average": deferred_lat_avg,
                "median": deferred_lat_med,
            },
            "reads": {
                "correct": deferred_correct_pct,
                "lost_update": deferred_wrong_pct
            }
        }
    }


def get_all_analysis(folder: str):
    all_analysis = []
    for filename in os.listdir(folder):
        with open(os.path.join(folder, filename), "r") as f:
            experiments = json.loads(f.read())
        analysis = get_analysis(experiments)
        all_analysis.append(analysis)
    return all_analysis


def main(args: List[str]):
    results_folder = args[0]
    analysis = get_all_analysis(f"results/{results_folder}")
    analysis.sort(key=lambda a: a["parameters"]["keyspace_size"])

    keyspace_sizes = [a["parameters"]["keyspace_size"] for a in analysis]

    # plot keyspace size x latency
    med_eager_lats = [a["eager"]["latency"]["median"] for a in analysis]
    med_deferred_lats = [a["deferred"]["latency"]["median"] for a in analysis]
    avg_eager_lats = [a["eager"]["latency"]["average"] for a in analysis]
    avg_deferred_lats = [a["deferred"]["latency"]["average"] for a in analysis]
    med_extra_lats = [deferred - eager for eager, deferred in zip(med_eager_lats, med_deferred_lats)]
    avg_extra_lats = [deferred - eager for eager, deferred in zip(avg_eager_lats, avg_deferred_lats)]
    plt.plot(keyspace_sizes, med_extra_lats, label="median deferred - median eager")
    plt.plot(keyspace_sizes, avg_extra_lats, label="average deferred - average eager")
    # plt.plot(keyspace_sizes, eager_lats, label="eager")
    # plt.plot(keyspace_sizes, deferred_lats, label="deferred")
    plt.xlabel("keyspace size")
    plt.ylabel("latency (s)")
    plt.legend()
    plt.savefig(f"plots/keyspace-latency-{results_folder}.png")
    plt.clf()
    plt.cla()
    plt.close()

    # plot keyspace size x lost updates
    eager_wrong_pct = [a["eager"]["reads"]["lost_update"] for a in analysis]
    deferred_wrong_pct = [a["deferred"]["reads"]["lost_update"] for a in analysis]
    plt.plot(keyspace_sizes, eager_wrong_pct, label="eager")
    plt.plot(keyspace_sizes, deferred_wrong_pct, label="deferred")
    plt.xlabel("keyspace size")
    plt.ylabel("% of lost updates")
    plt.legend()
    plt.savefig(f"plots/keyspace-lostupdates-{results_folder}.png")
    plt.clf()
    plt.cla()
    plt.close()


if __name__ == "__main__":
    main(sys.argv[1:])
