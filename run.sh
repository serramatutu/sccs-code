#/bin/bash

# uncapped
# poetry run python main.py \
#     -o results/uncapped.json \
#     --num-partitions 10 \
#     --keyspace-size 10 \
#     --duration 30 \
#     --tps 100 \
#     --execution-time-avg 1

poetry run python main.py \
    -o results/results-max-25.json \
    --num-partitions 10 \
    --keyspace-size 10 \
    --duration 30 \
    --tps 100 \
    --execution-time-avg 1 \
    --max-defer-time 25
