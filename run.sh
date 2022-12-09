#/bin/bash

poetry run python main.py \
    -o results.json \
    --num-partitions 10 \
    --keyspace-size 10 \
    --duration 30 \
    --tps 100 \
    --execution-time-avg 1
