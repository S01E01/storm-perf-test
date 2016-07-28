package com.vandt.storm.benchmarking.components;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class GatheringBolt extends BaseRichBolt
{
    private final boolean debug;
    private final boolean persistTuples;

    private static final long serialVersionUID = 1L;

    private static final List<Tuple> tuples = new ArrayList<>();
    private Integer count = 0;

    private transient TopologyContext context;
    private transient OutputCollector collector;


    public GatheringBolt() {
        this(true, true);
    }

    public GatheringBolt(boolean debug, boolean persistTuples) {
        this.debug = debug;
        this.persistTuples = persistTuples;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.context = context;
        this.collector = collector;
        tuples.clear();
    }

    @Override
    public void execute(Tuple input)
    {
        Tuple newTuple = new TupleImpl(context, input.getValues(), input.getSourceTask(), input.getSourceStreamId());

        if (debug) {
            System.out.println(this.getClass().toString() + " : Adding #" + this.count + " tuple = " + newTuple.getValues());
        }

        if (persistTuples) {
            tuples.add(newTuple);
        }

        count++;
        collector.ack(input);
    }

    public List<Tuple> getGatheredData()
    {
        return new ArrayList<Tuple>(tuples);
    }

    @Override
    public void cleanup() {
        System.out.println(this.getClass().toString() + " Gathered #" + count + " tuples");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
