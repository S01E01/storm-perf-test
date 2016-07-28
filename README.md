<!--
  Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
storm-perf-test
===============

Storm Benchmarking

Try this for examples benchmark: 

```
storm jar ./target/storm_perf_test-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.vandt.storm.benchmarking.Benching benching local null /results/ false
```

Very limited Benchmark tool to benchmark topologies locally or on a cluster and write results to disk in CSV format.

Benchmark topologies by implementing the BaseTopology and Benchmark interfaces.
Run solitary topologies with the BenchmarkTopology class and benchmarks consisting of multiple topologies with the Benching class.
