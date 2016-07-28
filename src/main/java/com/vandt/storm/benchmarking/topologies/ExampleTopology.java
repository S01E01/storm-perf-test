package com.vandt.storm.benchmarking.topologies;

import com.vandt.storm.benchmarking.components.GatheringBolt;
import com.vandt.storm.benchmarking.components.GitHubSpout;
import com.vandt.storm.benchmarking.components.PassOnBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ExampleTopology extends BaseTopology {

    public static void main(String[] args) throws TException {
        new ExampleTopology().realMain(args);
    }

    public StormTopology build(String datadir, boolean ackEnabled, int spoutParallelism, int decisionParallelism, int sinkParallelism, boolean debug) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        IRichSpout spout = new GitHubSpout(ackEnabled, "default", 1000);
        IRichBolt passon = new PassOnBolt();
        IRichBolt gatherer = new GatheringBolt(debug, false);

        topologyBuilder.setSpout("spout", spout, spoutParallelism);
        topologyBuilder.setBolt("passon", passon, decisionParallelism).fieldsGrouping("spout", new Fields("actor_id"));
        topologyBuilder.setBolt("gatherer", gatherer, sinkParallelism).fieldsGrouping("passon", new Fields("actor_id"));

        return topologyBuilder.createTopology();
    }
}
