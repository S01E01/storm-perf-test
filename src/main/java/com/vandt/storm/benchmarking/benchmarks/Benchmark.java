package com.vandt.storm.benchmarking.benchmarks;

import com.vandt.storm.benchmarking.BenchmarkConfig;
import com.vandt.storm.benchmarking.topologies.BaseTopology;

import java.util.List;

abstract public class Benchmark {

    abstract public List<BaseTopology> topologies();

    abstract public List<BenchmarkConfig> benchmarkConfigs();

    public List<BenchmarkConfig> benchmarkConfigs(String outputPath, String dataDir, String topoPrefix) {
        List<BenchmarkConfig> configs = benchmarkConfigs();

        String benchmarkName = this.getClass().getSimpleName() + "_" + System.nanoTime();

        for (BenchmarkConfig config : configs) {
            config.setBenchmarkName(benchmarkName);
            config.outputPath = outputPath;
            config.datadir = dataDir;
            config.setTopologyPrefix(topoPrefix);
        }

        return configs;
    }
}
