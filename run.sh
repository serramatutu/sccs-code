#/bin/bash

mkdir -p -- results/capped-10


for KEYSPACE_SIZE in `eval echo {10..1000..20}`; do
    poetry run python main.py \
        -o results/capped-10/$KEYSPACE_SIZE.json \
        --num-partitions 10 \
        --keyspace-size $KEYSPACE_SIZE \
        --duration 30 \
        --tps 100 \
        --execution-time-avg 1 \
        --max-defer-time 10
    
    echo "Ran for keyspace size of $KEYSPACE_SIZE"
done

# uncapped
# poetry run python main.py \
#     -o results/uncapped.json \
#     --num-partitions 10 \
#     --keyspace-size 500 \
#     --duration 30 \
#     --tps 100 \
#     --execution-time-avg 1

# poetry run python main.py \
#     -o results/max-10.json \
#     --num-partitions 10 \
#     --keyspace-size 500 \
#     --duration 30 \
#     --tps 100 \
#     --execution-time-avg 1 \
#     --max-defer-time 25
