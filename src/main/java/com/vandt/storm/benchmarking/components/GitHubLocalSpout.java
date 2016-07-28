package com.vandt.storm.benchmarking.components;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GitHubLocalSpout extends BaseRichSpout {

    protected final String datadir;

    protected final boolean debug;
    protected final boolean ackEnabled;
    protected final String streamId;
    protected transient GitHubLocalReader gitHubReader;
    protected SpoutOutputCollector collector;

    protected List<List<Object>> tuples;
    protected int index = 0;
    protected int eventsSize = 0;
    protected int messageCount = 0;

    public GitHubLocalSpout(String datadir, boolean ackEnabled) {
        this(datadir, ackEnabled, "default", true);
    }

    public GitHubLocalSpout(String datadir, boolean ackEnabled, String streamId, boolean debug) {
        this.datadir = datadir;
        this.ackEnabled = ackEnabled;
        this.streamId = streamId;
        this.debug = debug;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this.streamId, new Fields("id", "type", "actor_id", "repo_id", "payload"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.gitHubReader = new GitHubLocalReader(debug, datadir);

        this.tuples = new ArrayList<>();
        JsonObject event;
        while ((event = this.gitHubReader.nextEvent()) != null) {
            List<Object> tuple = toTuple(event);
            this.tuples.add(tuple);
        }
        eventsSize = this.tuples.size();
    }

    @Override
    public synchronized void nextTuple() {
        List<Object> tuple = getNextTuple();

        while (!tupleIsValid(tuple)) {
            tuple = getNextTuple();
        }

        if (tuple == null) {
            return;
        }


        if (this.ackEnabled) {
            this.collector.emit(this.streamId, tuple, messageCount++);
        } else {
            this.collector.emit(this.streamId, tuple);
        }
    }

    protected List<Object> getNextTuple() {
        if (index == eventsSize) {
            resetCycle();
        }

        return tuples.get(index++);
    }

    protected void resetCycle() {
        index = 0;
    }

    protected boolean tupleIsValid(List<Object> tuple) {
        return true;
    }

    protected List<Object> toTuple(JsonObject jsonObject) {
        try {
            JsonObject actor = jsonObject.getAsJsonObject("actor");
            JsonObject repo = jsonObject.getAsJsonObject("repo");


            return toTuple(
                    jsonObject.getAsJsonPrimitive("id").getAsLong(),
                    jsonObject.getAsJsonPrimitive("type").getAsString(),
                    actor.getAsJsonPrimitive("id").getAsLong(),
                    repo.getAsJsonPrimitive("id").getAsLong(),
                    jsonObject.get("payload").toString()
            );
        } catch (Exception e) {
            return null;
        }
    }

    protected List<Object> toTuple(Long id, String type, Long actorId, Long repoId, Object payload) {
        return new Values(id, type, actorId, repoId, payload);
    }


    private class GitHubLocalReader {
        private final boolean debug;
        private final String filePath;
        private int fileIndex = 0;
        private Iterator<JsonElement> eventIterator;

        public GitHubLocalReader(boolean debug, String filePath) {
            this.debug = debug;
            this.filePath = filePath;
        }

        public JsonObject nextEvent() {
            if (eventIterator != null && eventIterator.hasNext()) {
                return (JsonObject) eventIterator.next();
            } else {
                eventIterator = nextIterator();

                if (eventIterator != null && eventIterator.hasNext()) {
                    return (JsonObject) eventIterator.next();
                }

                return null;
            }
        }

        private Iterator<JsonElement> nextIterator() {
            String totalPath = filePath + "gh_" + fileIndex++ + ".json";

            if (debug) {
                System.out.println("Reading file: " + totalPath);
            }

            File file = new File(totalPath);

            if (!file.exists()) {

                if (debug) {
                    System.out.println("File '" + totalPath + "' does not exist. Done!");
                }

                return null;
            }

            try {
                InputStream inStream = new FileInputStream(file);
                InputStreamReader inReader = new InputStreamReader(inStream);
                JsonReader reader = new JsonReader(inReader);

                JsonArray jsonArray = new Gson().fromJson(reader, JsonArray.class);
                return jsonArray.iterator();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println(e + "; " + e.getLocalizedMessage());
                throw new RuntimeException(e);
            }
        }
    }
}
