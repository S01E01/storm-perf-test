package com.vandt.storm.benchmarking;

import com.vandt.storm.benchmarking.metrics.MetricLogging;
import com.vandt.storm.benchmarking.metrics.MetricState;

import com.vandt.storm.benchmarking.topologies.ExampleTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class BenchmarkTopology extends Benching {
    private final LocalCluster localCluster = new LocalCluster();
    private final BenchmarkConfig benchmarkConfig;


    public static void main(String[] args) throws Exception {
        new BenchmarkTopology(null).realMain(args);
    }

    public void realMain(String[] args) throws Exception {
        BenchmarkConfig config = new BenchmarkConfig(new ExampleTopology()); // Insert benchmark
        config.setTopologyPrefix(this.getTopologyPrefix(args));
        config.deploymentTactic = this.getDeploymentTactic(args);
        config.datadir = this.getDataDir(args);
        config.outputPath = this.getOutputPath(args);
        config.debug = this.getDebug(args);

        new BenchmarkTopology(config).run();
    }

    public BenchmarkTopology(BenchmarkConfig benchmarkConfig) {
        this.benchmarkConfig = benchmarkConfig;
    }

    public void run() throws Exception {
        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        try {
            StormTopology topology = benchmarkConfig.topology.build(
                    benchmarkConfig.datadir,
                    benchmarkConfig.isAckEnabled(),
                    benchmarkConfig.spoutParallelism,
                    benchmarkConfig.decisionParallelism,
                    benchmarkConfig.sinkParallelism,
                    benchmarkConfig.debug
            );

            Config config = new Config();
            config.setDebug(benchmarkConfig.debug);
            config.setNumAckers(benchmarkConfig.numAckers);
            config.setNumWorkers(benchmarkConfig.numWorkers);
            if (benchmarkConfig.maxSpoutPending > 0) {
                config.setMaxSpoutPending(benchmarkConfig.maxSpoutPending);
            }

            this.deployTopology(topology, config, benchmarkConfig);

            startLoggingMetrics(client, benchmarkConfig.pollFreqSec, benchmarkConfig.runningTimeSec);
//        } catch (Exception e) {
//            System.err.println(e);
        } finally {
            System.out.println("KILLING... " + benchmarkConfig.getTopologyName());

            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(1);

            try {
                this.killTopology(benchmarkConfig.getTopologyName(), client, killOpts, benchmarkConfig);
                Thread.sleep(killOpts.get_wait_secs() * 1000);
                this.shutdown(client, benchmarkConfig);
                System.out.println("KILLED " + benchmarkConfig.getTopologyName());
                System.out.println();
            } catch (Exception e) {
                System.err.println("Error tying to kill " + benchmarkConfig.getTopologyName() + ", with error: " + e);
            }
        }
    }

    protected void startLoggingMetrics(Nimbus.Client client, int pollFreqsSec, int runningTimeSec) throws Exception {
        MetricState state = MetricState.getInstance();
        state = state.reset(benchmarkConfig.statusWaiting);


        System.out.println();
        new MetricLogging().printHeaders(state);
        new MetricLogging().write(fileName(), benchmarkConfig.headers());
        new MetricLogging().write(fileName(), benchmarkConfig.settings());

        long pollFreqMs = pollFreqsSec * 1000;

        state.lastTiming = this.currentTimingMs();
        state.status = benchmarkConfig.statusWaiting;
        long totalRunTime = 0;

        while (totalRunTime < runningTimeSec) {
            long now = System.currentTimeMillis();

            // Log the metrics
            boolean topologyStarted = logMetrics(client, state);

            // Count runtime only if we are have properly started
            if (state.status.equals(benchmarkConfig.statusRunning)) {
                totalRunTime = (now - state.startTime) / 1000;
            }

            // Started proper
            if (state.status.equals(benchmarkConfig.statusWaiting) && topologyStarted) {
                new MetricLogging().printHeaders(state);
                new MetricLogging().writeHeaders(fileName(), state);

                state.status = benchmarkConfig.statusRunning;
                state.startTime = now;
                state.endTime = state.startTime + (runningTimeSec * 1000);
            }

            // Sleep till next poll
            if (state.endTime > now) {
                Thread.sleep(pollFreqMs);
            }
        }
    }

    public boolean logMetrics(Nimbus.Client client, MetricState state) throws Exception {

        // Hold last stats
        int lastSlotsUsed = state.slotsUsed;
        int lastExecutors = state.activeExecutors;

        // Update stats
        state.update(client, localCluster, benchmarkConfig.deploymentTactic);

        // Stats delta
        int slotsUsedDiff = state.slotsUsed - lastSlotsUsed;
        int executorsDiff = state.activeExecutors - lastExecutors;


        new MetricLogging().printCycleReport(state);

//        if (state.status.equals(benchmarkConfig.statusRunning)) {
            new MetricLogging().writeCycleReport(fileName(), state);
//        }


        if (benchmarkConfig.debug && state.status.equals(benchmarkConfig.statusWaiting)) {
            System.out.println("" + (state.totalSlots > 0 ? "true" : "false") +
                    ", " + (slotsUsedDiff == 0 ? "true" : "false") +
                    ", " + (state.totalExecutors > 0 ? "true" : "false") +
                    ", " + (state.activeExecutors == state.totalExecutors ? "true" : "false") +
                    ", " + (executorsDiff == 0 ? "true" : "false")
            );
        }

        // Running when ..
        return (state.slotsUsed > 0 &&
                slotsUsedDiff == 0 &&
                state.slotsUsed == benchmarkConfig.numWorkers &&
                state.totalExecutors > 0 &&
                state.activeExecutors == state.totalExecutors &&
                executorsDiff == 0
        );
    }

    protected Long currentTimingMs() throws Exception {
//        return this.timingUtils.timeToMillis(this.timingUtils.cpuTime());
        return System.currentTimeMillis();
    }

    protected String fileName() {
        return fileName(this.benchmarkConfig);
    }

    public String fileName(BenchmarkConfig benchmarkConfig) {
        String path = benchmarkConfig.outputPath;
        String benchName = benchmarkConfig.getBenchmarkName();
        String fileName = benchmarkConfig.getTopologyName();

        String fullName = path;

        if (benchName != null) {
            fullName += benchName + "/";
        }

        fullName += fileName + ".csv";

        return fullName;
    }

    protected void deployTopology(StormTopology topology, Config config, BenchmarkConfig benchConfig) throws TException {
        switch (benchConfig.deploymentTactic) {
            case "local":
                deployTopologyLocal(topology, config, benchConfig.getTopologyName());
                break;
            case "cluster":
                deployTopologyClustered(topology, config, benchConfig.getTopologyName());
                break;
            default:
                deployTopologyLocal(topology, config, benchConfig.getTopologyName());
                break;
        }
    }

    protected void deployTopologyLocal(StormTopology topology, Config config, String name) {
        this.localCluster.submitTopology(name, config, topology);
    }

    protected void deployTopologyClustered(StormTopology topology, Config config, String name) throws TException {
        StormSubmitter.submitTopologyWithProgressBar(name, config, topology);
    }

    protected void killTopology(String name, Nimbus.Client client, KillOptions killOptions, BenchmarkConfig benchConfig) throws TException {
        switch (benchConfig.deploymentTactic) {
            case "cluster":
                killTopologyClustered(name, client, killOptions);
                break;
            case "local":
            default:
                killTopologyLocal(name, killOptions);
                break;
        }
    }

    protected void killTopologyLocal(String name, KillOptions killOptions) {
        this.localCluster.killTopologyWithOpts(name, killOptions);
    }

    protected void killTopologyClustered(String name, Nimbus.Client client, KillOptions killOptions) throws TException {
        client.killTopologyWithOpts(name, killOptions);
    }

    protected void shutdown(Nimbus.Client client, BenchmarkConfig benchConfig) {
        switch (benchConfig.deploymentTactic) {
            case "cluster":
                // no-op
                break;
            case "local":
            default:
                shutdownLocal();
                break;
        }
    }

    protected void shutdownLocal() {
        this.localCluster.shutdown();
    }
}
