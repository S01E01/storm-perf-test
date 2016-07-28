package com.vandt.storm.benchmarking.metrics;

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;

import java.util.HashMap;
import java.util.Map;

public class MetricState {

    public String status;
    public int topologies = 0;
    public Map<String, Long> transferred = new HashMap<>();
    public Map<String, Long> lastTransferred = new HashMap<>();
    public Map<String, Long> failed = new HashMap<>();
    public long totalLatency = 0;
    public long lastTotalLatency = 0;
    public long totalSpoutAcked = 0;
    public long lastTotalSpoutAcked = 0;
    public long timing = 0;
    public long lastTiming = 0;
    public int totalSlots = 0;
    public int slotsUsed = 0;
    public int totalExecutors = 0;
    public int activeExecutors = 0;

    public long startTime = 0;
    public long endTime = Long.MAX_VALUE;

    private static MetricState instance = null;

    public static MetricState getInstance() {
        if(instance == null) {
            instance = new MetricState();
        }
        return instance;
    }

    public MetricState reset(String status) {
        instance = new MetricState();
        instance.status = status;
        return instance;
    }

    public long getTimingDiff() {
        if (lastTiming == 0) {
            return 0;
        }

        return timing - lastTiming;
    }

    public Map<String, Long> getTransferredDiff() {
        Map<String, Long> diff = new HashMap<>();
        for (String key : transferred.keySet()) {
            Long diffVal = 0L;

            if (lastTransferred.containsKey(key)) {
                diffVal = transferred.get(key) - lastTransferred.get(key);
            }

            diff.put(key, diffVal);
        }
        return diff;
    }

    public Map<String, Long> getLastCycleThroughput() {
        long timingDiff = getTimingDiff();
        Map<String, Long> transferredDiff = getTransferredDiff();
        Map<String, Long> cycleThroughput = new HashMap<>();

        for (Map.Entry<String, Long> e : transferredDiff.entrySet()) {
            if (timingDiff == 0 || e.getValue() == 0) {
                cycleThroughput.put(e.getKey(), 0L);
            } else {
                cycleThroughput.put(e.getKey(), (long) (e.getValue() / (timingDiff / 1000.0)));
            }
        }

        return cycleThroughput;
    }

    public long getTotalLatencyDiff() {
        return totalLatency - lastTotalLatency;
    }

    public long getSpoutAckedDiff() {
        return totalSpoutAcked - lastTotalSpoutAcked;
    }

    public Double getLastCycleAvgMs() {
        double timingDiff = getTimingDiff();
        double totalLatencyDiff = getTotalLatencyDiff();
        double spoutAckedDiff = getSpoutAckedDiff();

        if (timingDiff == 0 || totalLatencyDiff == 0 || spoutAckedDiff == 0) {
            return 0D;
        }

        return (totalLatencyDiff / spoutAckedDiff);
    }

    public Long totalCount(Map<String, Long> map) {
        Long total = 0L;
        for (Long value : map.values()) {
            total += value;
        }
        return total;
    }

    public void update(Nimbus.Client client, LocalCluster local, String deploymentTactic) throws Exception {
        ClusterSummary summary = getSummaryForDeploymentTactic(client, local, deploymentTactic);
        topologies = summary.get_topologies_size();

        // Update run timings
        lastTiming = timing;
        timing = System.currentTimeMillis();

        // Update slots (i.e. workers)
        totalSlots = 0;
        slotsUsed = 0;
        for (SupervisorSummary sup : summary.get_supervisors()) {
            totalSlots += sup.get_num_workers();
            slotsUsed += sup.get_num_used_workers();
        }

        // Update executors and transferred events
        lastTransferred = duplicateMap(transferred);
        lastTotalLatency = totalLatency;
        lastTotalSpoutAcked = totalSpoutAcked;
        transferred = new HashMap<>();
        totalLatency = 0;
        totalSpoutAcked = 0;
        totalExecutors = 0;
        activeExecutors = 0;
        for (TopologySummary ts : summary.get_topologies()) {
            String id = ts.get_id();
            TopologyInfo info = getTopologyInfoForDeploymentTactic(client, local, id, deploymentTactic);
            if (info == null) {
                continue;
            }

            for (ExecutorSummary es : info.get_executors()) {
                ExecutorStats stats = es.get_stats();
                String componentId = es.get_component_id();
                totalExecutors++;


                if (stats != null) {

                    // Get total failed
                    if (stats.get_specific().is_set_spout()) {
                        SpoutStats ss = stats.get_specific().get_spout();

                        Long failed = getCountFromStatsProcessedMap(ss.get_failed(), ":all-time");
                        this.failed.put(componentId, failed != null ? failed : 0L);

                        Long acked = getCountFromStatsProcessedMap(ss.get_acked(), ":all-time");
                        Double avgMS = getCountFromStatsProcessedMap(ss.get_complete_ms_avg(), ":all-time");

                        if (acked != null && avgMS != null) {
                            totalLatency += Math.round(acked * avgMS);
                            totalSpoutAcked += acked;
                        }
                    }

                    if (executorIsActive(stats)) {
                        activeExecutors++;
                    }

                    // Get total transferred
                    Long transferred = getCountFromStatsProcessedMap(stats.get_transferred(), ":all-time");
                    if (transferred != null) {

                        // Add previously inserted values for this componentId
                        // When run in parallel there will be more than one for each component
                        if (this.transferred.containsKey(componentId)) {
                            transferred += this.transferred.get(componentId);
                        }

                        this.transferred.put(componentId, transferred);
                    }
                }
            }
        }
    }

    protected <T> T getCountFromStatsProcessedMap(Map<String, Map<String, T>> map, String key) {
        if (map != null) {
            Map<String, T> kMap = map.get(key);

            if (kMap != null) {
                for (String streamId : kMap.keySet()) {
                    if (!streamId.startsWith("__")) {
                        return kMap.get(streamId);
                    }
                }
            }
        }

        return null;
    }

    protected boolean executorIsActive(ExecutorStats stats) {
        if (stats == null) {
            return false;
        }

        if (stats.get_transferred_size() > 0 || stats.get_emitted_size() > 0) {
            return true;
        }

        ExecutorSpecificStats specifics = stats.get_specific();
        if (specifics == null || !specifics.isSet()) {
            return false;
        }

        if (specifics.is_set_bolt()) {
            BoltStats boltStats = specifics.get_bolt();
            if (boltStats != null) {
                return boltStats.get_acked_size() > 0 || boltStats.get_executed_size() > 0;
            }
        }

        if (specifics.is_set_spout()) {
            SpoutStats spoutStats = specifics.get_spout();
            if (spoutStats != null) {
                return spoutStats.get_acked_size() > 0;
            }
        }

        return false;
    }

    protected Map<String, Long> duplicateMap(Map<String, Long> map) {
        Map<String, Long> newMap = new HashMap<>();
        for (Map.Entry<String, Long> e : map.entrySet()) {
            newMap.put(e.getKey(), e.getValue());
        }
        return newMap;
    }

    protected TopologyInfo getTopologyInfoForDeploymentTactic(Nimbus.Client client, LocalCluster local, String topologyId, String deploymentTactic) throws TException {
        switch (deploymentTactic) {
            case "cluster":
                return client != null ? client.getTopologyInfo(topologyId) : null;
            case "local":
            default:
                return local != null ? local.getTopologyInfo(topologyId) : null;
        }
    }

    protected ClusterSummary getSummaryForDeploymentTactic(Nimbus.Client client, LocalCluster local, String deploymentTactic) throws TException {
        switch (deploymentTactic) {
            case "cluster":
                return client.getClusterInfo();
            case "local":
            default:
                return local.getClusterInfo();
        }
    }

}
