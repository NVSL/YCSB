#!/bin/bash

function sz_env {
source /root/YJ/SubzeroApps/setenv.sh nova
}

function build {

bin=$1
cd /root/YJ/kyotocabinet_$bin
make clean && make -j`nproc` && sudo make install

cd /root/YJ/kyotocabinet-java-1.24
make clean && make -j`nproc` && sudo make install
}

function run {
for i in {20..27..2}; do
    echo $1_$((2 ** $i))
    ./kyotocabinet/kcycsb.py --fieldlength $((2 ** $i)) --fieldcount 1 --logdir $1_$((2 ** $i))
done
}

# build patch
run patch
