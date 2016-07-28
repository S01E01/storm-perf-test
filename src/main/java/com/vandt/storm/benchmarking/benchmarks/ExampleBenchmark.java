package com.vandt.storm.benchmarking.benchmarks;

import com.vandt.storm.benchmarking.BenchmarkConfig;
import com.vandt.storm.benchmarking.topologies.BaseTopology;
import com.vandt.storm.benchmarking.topologies.ExampleTopology;

import java.util.ArrayList;
import java.util.List;

public class ExampleBenchmark extends Benchmark {

    public List<BaseTopology> topologies() {
        List<BaseTopology> topologies = new ArrayList<>();
        topologies.add(new ExampleTopology());
        return topologies;
    }


    public List<BenchmarkConfig> benchmarkConfigs() {
        List<BenchmarkConfig> configs = new ArrayList<>();

        for (BaseTopology topology : topologies()) {
            BenchmarkConfig config = new BenchmarkConfig(topology);

            config.debug = false;
            config.pollFreqSec = 50;
            config.runningTimeSec = 600;
            config.numAckers = 0;
            config.numWorkers = 1;
            config.maxSpoutPending = -1;
            config.spoutParallelism = 1;
            config.decisionParallelism = 1;
            config.sinkParallelism = 1;
            config.deploymentTactic = "cluster";

            configs.add(config);
        }

        return configs;
    }
}
