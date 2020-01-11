#!/bin/bash

for i in {12..18}; do
    ./kyotocabinet/kcycsb.py --fieldlength $((2 ** $i)) --fieldcount 1 --logdir patch_$((2 ** $i))
done