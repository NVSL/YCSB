#!/bin/bash

function sz_env {
source ../SubzeroApps/setenv.sh nova
}

function build {

cd ../kyotocabinet_$1
make clean && make -j`nproc` && sudo make install

cd ../kyotocabinet-java-1.24
make clean && make -j`nproc` && sudo make install

cd ../YCSB
mvn -pl site.ycsb:kyotocabinet-binding -am clean package

}

function run {
for i in {20..21..2}; do
    echo $1_$((2 ** $i))
    ./kyotocabinet/kcycsb.py --fieldlength $((2 ** $i)) --fieldcount 1 --logdir $1_$((2 ** $i))
done
}

#build patch
run patch
