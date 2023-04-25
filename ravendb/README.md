<!--
Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on RavenDB.

###  RavenDB Configuration Parameters

* `ravendb.host` (required): defines the RavenDB endpoint, format `http://IP`, the binding uses the default port `8080`

* `port` (optional, default=8080):  the port that the RavenDB database listens on

* `ravendb.dbname` (required):  the name of the RavenDB database to use for the YCSB, e.g. `ycsb`

* `batchsize` (optional, default=1):  batch insert size, e.g. `1000`
* `ravendb.maxRequests` (optional, default=100):  increased the maximum number of requests to execute concurrently.
* `ravendb.maxRequestsPerHost` (optional, default=100):  increased the maximum number of requests for each host to execute concurrently. 
* `useOptimisticConcurrency` (optional, default=false):  use optimistic concurrency

* `debug` (optional, default=false):  print debug messages

### Run YCSB

Now you are ready to run! First, use the asynchronous driver to load the data:

    ./bin/ycsb load ravendb -s -P workloads/workloada > outputLoad.txt

Then, run the workload:

    ./bin/ycsb run ravendb -s -P workloads/workloada > outputRun.txt