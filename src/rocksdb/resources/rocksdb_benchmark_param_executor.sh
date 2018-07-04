#!/bin/bash

# This script aims at evaluating different values for RocksDB parameters for performance tunning.
# It assumes to be run in the same directory as RocksDB, as it relaies on the benchamrking tool
# already available for RocksDB (./db_bench).

# List of parameters that will be object of execution
write_buffer_size='write_buffer_size=67108864 write_buffer_size=268435456 write_buffer_size=536870912'
max_write_buffer_number='max_write_buffer_number=5 max_write_buffer_number=20'
block_size='block_size=4096 block_size=8192 block_size=16384'

benchmark='readwhilewriting'
# In the default execution mode, we disable the write ahead log (WAL) as well as sync writes
execution_command='./db_bench --benchmarks='$benchmark' --statistics=1 --histogram=1 --num=1000000 --sync=0 --disable_wal=1'
current_date=$(date '+%Y-%m-%d_%H-%M-%S')
output_file='results'$current_date'.txt'

# Iterate over the important parameters (be careful, the number of combinations grows quickly)
for wbs in $write_buffer_size; do
    for mwbn in $max_write_buffer_number; do
        for bs in $block_size; do

            # Execute the benchmark with the specific configuration
            for iteration in 1; do
                experiment_id='results_'$wbs'_'$mwbn'_'$bs'_'$iteration
                echo $experiment_id >> $output_file

                to_execute=$execution_command' --'$wbs' --'$mwbn' --'$bs' >> '$output_file
                echo $to_execute
                eval $to_execute
            done

        done
    done
done