<!--
Copyright (c) 2012 - 2018 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

Kyotocabinet
Using kyotocabinet-java wrapper.

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

Or,

```shell
    mvn -pl site.ycsb:redis-binding -am clean package
```

### 2. Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data

Then, run the workload:

    ./bin/ycsb run rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data

## RocksDB Configuration Parameters

* ```kyoto.dir``` - (required) A path to a folder to hold the Kyotocabinet data files.
    * EX. ```/tmp/ycsb-rocksdb-data```

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load kyotocabinet -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run kyotocabinet -s -P workloads/workloada > outputRun.txt