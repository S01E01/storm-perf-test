package com.vandt.storm.benchmarking;

import com.vandt.storm.benchmarking.benchmarks.ExampleBenchmark;
import com.vandt.storm.benchmarking.topologies.BaseTopology;
import com.vandt.storm.benchmarking.metrics.MetricLogging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Benching {

    public static void main(String[] args) throws Exception {
        new Benching().realMain(args);
    }

    public void realMain(String[] args) throws Exception {
        String topologyPrefix = this.getTopologyPrefix(args);
        String deploymentTactic = this.getDeploymentTactic(args);
        String datadir = this.getDataDir(args);
        String outputPath = this.getOutputPath(args);
        boolean debug = this.getDebug(args);


        List<BaseTopology> topologies = new ArrayList<>();
        List<BenchmarkConfig> benchConfigs = new ArrayList<>();

        // Add topologies or benchmark configs
        benchConfigs.addAll(new ExampleBenchmark().benchmarkConfigs(outputPath, datadir, topologyPrefix));

        for (BaseTopology topology : topologies) {

            for (int i = 1; i <= 1; i++) {
                BenchmarkConfig config = new BenchmarkConfig(topology);

                config.spoutParallelism = 1;
                config.decisionParallelism = i;
                config.sinkParallelism = 1;

                config.debug = debug;
                config.runningTimeSec = 120;
                config.deploymentTactic = deploymentTactic;
                config.outputPath = outputPath;
                config.datadir = datadir;
                config.setTopologyPrefix(topologyPrefix);
                benchConfigs.add(config);
            }
        }


        for (BenchmarkConfig benchConfig : benchConfigs) {
            BenchmarkTopology benchmarkTopology = new BenchmarkTopology(benchConfig);

            try {
                benchmarkTopology.run();
            } catch (Exception e) {
                e.printStackTrace();

                // Log error
                try {
                    MetricLogging logging = new MetricLogging();

                    logging.write(
                            benchmarkTopology.fileName(benchConfig),
                            e.toString(),
                            e.getLocalizedMessage()
                    );
                    logging.write("Stacktrace:");
                    for (StackTraceElement stackElement : e.getStackTrace()) {
                        logging.write(stackElement.toString());
                    }
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }
    }

    protected String getTopologyPrefix(String[] args) {
        if (args != null && args.length > 0) {
            return args[0];
        }

        return "benching_topo";
    }

    protected String getDeploymentTactic(String[] args) {
        if (args != null && args.length > 1) {
            switch (args[1]) {
                case "cluster":
                case "clustered":
                    return "cluster";
                case "local":
                case "localhost":
                default:
                    return "local";
            }
        }

        return "local";
    }

    protected String getDataDir(String[] args) {
        if (args != null && args.length > 2) {
            return args[2];
        }

        return null;
    }

    protected String getOutputPath(String[] args) {
        if (args != null && args.length > 3) {
            return args[3];
        }

        return "/Users/vandt/Documents/";
    }

    protected boolean getDebug(String[] args) {
        if (args != null && args.length > 4) {
            if (args[4].equals("debug") || args[4].equals("true")) {
                return true;
            }
        }

        return new BenchmarkConfig(null).debug;
    }
}
