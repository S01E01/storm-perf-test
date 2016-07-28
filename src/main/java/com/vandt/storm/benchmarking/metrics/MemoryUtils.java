package com.vandt.storm.benchmarking.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;


public class MemoryUtils {
    public enum FormatBytes {
        B, KB, MB, GB
    }

    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    public long heapMemoryUsed() {
        return memoryBean.getHeapMemoryUsage().getUsed();
    }

    public long heapMemoryUsed(FormatBytes format) {
        return formatBytes(heapMemoryUsed(), format);
    }

    public long nonHeapMemoryUsed() {
        return memoryBean.getNonHeapMemoryUsage().getUsed();
    }

    public long nonHeapMemoryUsed(FormatBytes format) {
        return formatBytes(nonHeapMemoryUsed(), format);
    }

    public long totalMemoryUsed() {
        return heapMemoryUsed() + nonHeapMemoryUsed();
    }

    public long totalMemoryUsed(FormatBytes format) {
        return formatBytes(totalMemoryUsed(), format);
    }

    private long formatBytes(long bytes, FormatBytes format) {
        switch (format) {
            default:
            case B:
                return bytes;
            case KB:
                return (bytes >> 10);
            case MB:
                return (bytes >> 10 >> 10);
            case GB:
                return (bytes >> 10 >> 10 >> 10);
        }
    }
}
