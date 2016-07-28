package com.vandt.storm.benchmarking.spout;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import org.eclipse.egit.github.core.client.GitHubResponse;


public class GitHubSpout extends BaseRichSpout {
    final String kGitHubUrl = "https://api.github.com/events";
    final String kClientId = "d64437f2ab6bce4fa137";
    final String kClientSecret = "fec898aebbaa0e1661a68bce03558ddcb7e6c51f";

    String eTag = null;

    private boolean ackEnabled;
    private SpoutOutputCollector collector;
    private final String streamId;
    private final Integer waitMillis;
    private Queue<List<Object>> tupleQueue = new LinkedList<>();


    public GitHubSpout() {
        this(-1);
    }

    public GitHubSpout(boolean ackEnabled) {
        this(ackEnabled, "default", -1);
    }

    public GitHubSpout(Integer eps) {
        this(true, "default", eps);
    }

    public GitHubSpout(boolean ackEnabled, String streamId, Integer eps) {
        this.ackEnabled = ackEnabled;
        this.streamId = streamId;
        this.waitMillis = 1000 / eps;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this.streamId, new Fields("id", "type", "actor_id", "repo_id", "payload"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        fetchGithubInBackground();
    }

    @Override
    public synchronized void nextTuple() {
        List<Object> nextTuple = this.tupleQueue.poll();

        if (nextTuple != null) {
            if (this.waitMillis > 0) {
                Utils.sleep(this.waitMillis);
            }

            if (this.ackEnabled) {
                this.collector.emit(this.streamId, nextTuple, nextTuple.get(0));
            } else {
                this.collector.emit(this.streamId, nextTuple);
            }
        } else {
            fetchGithubInBackground();
        }
    }

    private void fetchGithubInBackground() {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                fetchGithub(1000 / waitMillis);
            }
        };
        Thread t = new Thread(r);
        t.start();
    }

    private synchronized void fetchGithub(Integer maxItems) {
        String uri = kGitHubUrl + "?client_id=" + kClientId + "&client_secret=" + kClientSecret;
        GitHubResponse gitHubResponse = null;

        do {
            try {
                URL url;
                if (gitHubResponse == null) {
                    url = new URL(uri);
                } else {
                    url = new URL(gitHubResponse.getNext());
                }


                HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();

                if (eTag != null) {
                    urlConnection.setRequestProperty("If-None-Match", this.eTag);
                }

                gitHubResponse = new GitHubResponse(urlConnection, urlConnection.getContent());

                String status = gitHubResponse.getHeader("Status");
                if (status != null && status.compareToIgnoreCase("200 OK") == 0) {
                    parseGitHubEvents(gitHubResponse);

                    this.eTag = gitHubResponse.getHeader("ETAG");
                }

//                System.out.println(this.tupleQueue.size());
            } catch (Exception e) {
                return;
            }
        } while (gitHubResponse.getNext() != null);


        if (this.tupleQueue.size() < maxItems) {
            fetchGithub(maxItems);
        }
    }

    private void parseGitHubEvents(GitHubResponse gitHubResponse) throws Exception {
        InputStream inputStream = (InputStream) gitHubResponse.getBody();

        JSONParser parser = new JSONParser();

        JSONArray jsonArray = (JSONArray) parser.parse(convertStreamToString(inputStream));

        Iterator<JSONObject> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            JSONObject jsonObject = iterator.next();

            if (jsonObject == null) {
                continue;
            }

            List<Object> tuple = toTuple(jsonObject);

            if (tuple == null) {
                continue;
            }

            this.tupleQueue.add(tuple);
        }
    }

    private List<Object> toTuple(JSONObject jsonObject) {
        try {
            JSONObject actor = (JSONObject) jsonObject.get("actor");
            JSONObject repo = (JSONObject) jsonObject.get("repo");

            return new Values(
                    jsonObject.get("id"),
                    jsonObject.get("type"),
                    actor.get("id"),
                    repo.get("id"),
                    ""//jsonObject.get("payload")
            );
        } catch (Exception e) {
            return null;
        }
    }

    public String convertStreamToString(InputStream is) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }
}
