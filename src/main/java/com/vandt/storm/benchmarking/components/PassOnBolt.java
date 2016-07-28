package com.vandt.storm.benchmarking.components;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PassOnBolt extends BaseRichBolt {
    private transient OutputCollector collector;

    public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        collector.emit("default", toOutputTuple(input));
        collector.ack(input);
    }

    private Values toOutputTuple(Tuple tuple) {
        Values vals = new Values();
        for (Object val : tuple.getValues()) {
            vals.add(val);
        }
        return vals;
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("default", new Fields("id", "type", "actor_id", "repo_id", "payload"));
    }
}