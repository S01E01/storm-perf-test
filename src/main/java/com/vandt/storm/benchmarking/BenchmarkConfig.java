package com.vandt.storm.benchmarking;

import com.vandt.storm.benchmarking.topologies.BaseTopology;

import java.util.ArrayList;
import java.util.List;

public class BenchmarkConfig {
    public final String statusWaiting = "WAITING";
    public final String statusRunning = "RUNNING";

    public final BaseTopology topology;

    public BenchmarkConfig(BaseTopology topology) {
        this.topology = topology;
    }

    public boolean debug = false;
    public Integer pollFreqSec = 10;
    public Integer runningTimeSec = 120;
    public Integer numAckers = 0;
    public Integer numWorkers = 1;
    public Integer maxSpoutPending = -1;
    public Integer spoutParallelism = 1;
    public Integer decisionParallelism = 1;
    public Integer sinkParallelism = 1;

    private String topologyPrefix;
    private String topologyName;
    private String benchmarkName;
    public String deploymentTactic = "local";
    public String datadir = "C:/Users/vandt/Dropbox/Master Project/Gh data/";
    public String outputPath = "/Users/vandt/Documents/";

    public boolean isAckEnabled() {
        return numAckers > 0;
    }

    public String getTopologyPrefix() {
        return topologyPrefix;
    }

    public void setTopologyPrefix(String topologyPrefix) {
        this.topologyPrefix = topologyPrefix;
        this.topologyName = null; // Reset topology name
    }

    public String getTopologyName() {
        if (this.topologyName != null) {
            return this.topologyName;
        }

        this.topologyName = createTopologyName();

        return this.topologyName;
    }

    private String createTopologyName() {
        String name = "";

        if (this.topologyPrefix != null && this.topologyPrefix.length() > 0) {
            name = this.topologyPrefix + "_";
        }

        name += this.topology.getClass().getSimpleName() + "_";
        name += System.nanoTime();

        return name;
    }

    public String getBenchmarkName() {
        return benchmarkName;
    }

    public void setBenchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    public List<String> headers() {
        ArrayList<String> headers = new ArrayList();
        headers.add("ackers");
        headers.add("workers");
        headers.add("spout pending");
        headers.add("spout parallel");
        headers.add("bolt parallel");
        headers.add("sink parallel");
        headers.add("deployment tactic");
        return headers;
    }

    public List<String> settings() {
        ArrayList<String> settings = new ArrayList();
        settings.add(Integer.toString(numAckers));
        settings.add(Integer.toString(numWorkers));
        settings.add(Integer.toString(maxSpoutPending));
        settings.add(Integer.toString(spoutParallelism));
        settings.add(Integer.toString(decisionParallelism));
        settings.add(Integer.toString(sinkParallelism));
        settings.add(deploymentTactic);
        return settings;
    }
}