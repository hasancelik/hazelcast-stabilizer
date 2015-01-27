package com.hazelcast.stabilizer.visualiser.data;

import com.hazelcast.stabilizer.visualiser.data.BenchmarkResults;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EventListener;
import java.util.EventObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Model {
    private final Map<String, BenchmarkResults> benchmarks;
    private final List<BenchmarkChangeListener> listeners;

    public Model() {
        benchmarks = new HashMap<String, BenchmarkResults>();
        listeners = new ArrayList<BenchmarkChangeListener>();
    }

    public void addBenchmarkResults(BenchmarkResults benchmarkResults) {
        String name = benchmarkResults.getName();
        benchmarks.put(name, benchmarkResults);
        changed(name);
    }

    public Set<String> getBenchmarkNames() {
        return Collections.unmodifiableSet(benchmarks.keySet());
    }

    public BenchmarkResults getBenchmarkResults(String name) {
        return benchmarks.get(name);
    }

    public void addBenchmarkChangeListener(BenchmarkChangeListener listener) {
        listeners.add(listener);
    }

    private void changed(String name) {
        for (BenchmarkChangeListener listener : listeners) {
            listener.benchmarkChanged(name);
        }
    }

    public static class BenchmarkedChanged extends EventObject {
        private final String name;

        public BenchmarkedChanged(Object source, String name) {
            super(source);
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public interface BenchmarkChangeListener extends EventListener {
        void benchmarkChanged(String benchmarkName);
    }

}
