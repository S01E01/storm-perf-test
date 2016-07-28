package com.vandt.storm.benchmarking.metrics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MetricLogging{

    public void writeCycleReport(String file, MetricState state) throws IOException {
        write(file, report(state));
    }

    public void printCycleReport(MetricState state) {
        List<String> report = report(state);

        String lineOut =
                report.get(0) + "\t" +
                report.get(1) + "\t\t" +
                report.get(2) + "\t\t" +
                report.get(3) + "\t\t" +
                report.get(4) + "\t\t" +
                report.get(5) + "\t\t" +
                report.get(6) + "\t" +
                report.get(7) + "\t\t" +
                report.get(8) + "\t\t" +
                report.get(9) + "\t\t\t" +
                report.get(10) + "\t" +
                report.get(11) + "\t"
                ;

        // Print the rest
        for (int i = 12; i < report.size(); i++) {
            lineOut += report.get(i) + "\t";
        }

        System.out.println(lineOut);
    }

    public void writeHeaders(String file, MetricState state) throws  IOException {
        write(file, headers(state));
    }

    public void printHeaders(MetricState state) {
        List<String> headers = headers(state);

        String headerLine = "";

        for (String header : headers) {
            headerLine += header + "\t";
        }

        System.out.println(headerLine);
    }

    private List<String> headers(MetricState state) {
        ArrayList<String> report = new ArrayList<>();
        report.add("status");
        report.add("topologies");
        report.add("totalSlots");
        report.add("slotsUsed");
        report.add("totalExecutors");
        report.add("activeExecutors");
        report.add("time");
        report.add("time-diff ms");
        report.add("total transferred");
        report.add("total (e/s)");
        report.add("latency (ms)");
        report.add("total Failed");
        report.add("heap size MB");
        report.add("int. results");

        for (String componentId : state.transferred.keySet()) {
            report.add(componentId + " (e/s)");
        }
        for (String componentId : state.failed.keySet()) {
            report.add(componentId + " failed");
        }

        return report;
    }

    private List<String> report(MetricState state) {
        Map<String, Long> transferredDiff = state.getTransferredDiff();
        Map<String, Long> cycleThroughput = state.getLastCycleThroughput();
        Map<String, Long> failed = state.failed;

        ArrayList<String> report = new ArrayList<>();
        report.add(state.status);
        report.add(Integer.toString(state.topologies));
        report.add(Integer.toString(state.totalSlots));
        report.add(Integer.toString(state.slotsUsed));
        report.add(Integer.toString(state.totalExecutors));
        report.add(Integer.toString(state.activeExecutors));
        report.add(Long.toString(state.timing));
        report.add(Long.toString(state.getTimingDiff()));
        report.add(Long.toString(state.totalCount(transferredDiff)));
        report.add(Long.toString(state.totalCount(cycleThroughput)));
        report.add(String.format("%.2f", state.getLastCycleAvgMs()));
        report.add(Long.toString(state.totalCount(failed)));
        report.add(Long.toString(new MemoryUtils().heapMemoryUsed(MemoryUtils.FormatBytes.MB)));
        report.add(Long.toString(state.intermediateResults));

        for (Long componentThroughput : cycleThroughput.values()) {
            report.add(Long.toString(componentThroughput));
        }
        for (Long componentFailed : failed.values()) {
            report.add(Long.toString(componentFailed));
        }

        return report;
    }

    public void write(String file, String... items) throws IOException {
        write(file, Arrays.asList(items));
    }

    public void write(String fileName, List<String> items) throws IOException {
        File file = new File(fileName);
        File parentDir = file.getParentFile();

        if (parentDir != null && !parentDir.isDirectory()) {
            parentDir.mkdir();
        }

        Writer writer = new FileWriter(file, true);

        int itemsCount = items.size();

        for (int i = 0; i < itemsCount; i++) {
            writer.append(items.get(i));

            if (i < itemsCount-1) {
                writer.append(",");
            }
        }

        writer.append(System.lineSeparator());
        writer.close();
    }
}
