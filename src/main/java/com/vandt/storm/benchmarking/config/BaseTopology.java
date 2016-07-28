package com.vandt.storm.benchmarking.config;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.thrift.TException;

abstract public class BaseTopology {

    protected String topologyName = "topology";
    protected boolean runClustered = false;
    protected boolean ackEnabled = false;
    protected int spoutParallelism = 1;
    protected int decisionParallelism = 1;
    protected int sinkParallelism = 1;
    protected boolean debug = false;


    public StormTopology build() {
        return build();
    }

    public StormTopology build(boolean debug) {
        return build(null, true, 1, 1, 1, debug);
    }

    abstract public StormTopology build(String datadir, boolean ackEnabled, int spoutParallelism, int decisionParallelism, int sinkParllelism, boolean debug);


    public void realMain(String[] args) throws TException {
        final String topologyName = this.getTopologyName(args);
        final boolean runClustered = this.getRunClusteredFlag(args);
        final int spoutParallalism = this.getSpoutParallalism(args);
        final int decisionParallalism = this.getDecisionParallelism(args);
        final boolean debug = this.getDebugFlag(args);
        final String dataDir = this.getDataDir(args);


        StormTopology topology = this.build(dataDir, this.ackEnabled, spoutParallalism, decisionParallalism, sinkParallelism, debug);

        Config conf = new Config();
        conf.setDebug(debug);
        conf.setNumAckers(1);
        conf.setNumWorkers(1);

        if (runClustered) {
            StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topologyName, conf, topology);
        }
    }

    protected String getTopologyName(String[] args) {
        int idx = 0;

        if (args != null && args.length > idx) {
            return args[idx];
        }

        return this.topologyName;
    }

    protected boolean getRunClusteredFlag(String[] args) {
        int idx = 1;

        if (args != null && args.length > idx) {
            switch (args[idx]) {
                case "cluster":
                case "clustered":
                    return true;
                case "local":
                case "localhost":
                default:
                    return false;
            }
        }

        return this.runClustered;
    }

    protected int getSpoutParallalism(String[] args) {
        return getIntAtIdx(args, 2, this.spoutParallelism);
    }

    protected int getDecisionParallelism(String[] args) {
        return getIntAtIdx(args, 3, this.decisionParallelism);
    }

    protected int getIntAtIdx(String[] args, int idx, int defaultVal) {
        if (args != null && args.length > idx) {
            return Integer.valueOf(args[idx]);
        }

        return defaultVal;
    }

    protected boolean getDebugFlag(String[] args) {
        int idx = 4;

        if (args != null && args.length > idx) {
            if (args[idx].equals("debug")) {
                return true;
            }
        }

        return this.debug;
    }

    protected String getDataDir(String[] args) {
        int idx = 5;

        if (args != null && args.length > idx) {
            return args[idx];
        }

        return null;
    }
}
