package ru.gitverse.bizzareowl.mapreduce.tasks;

import java.util.List;

@FunctionalInterface
public interface ReduceTask {

    default ReduceTask defaultReduce() {
        return (key, values) -> {
           return null;
        };
    }

    String reduce(String key, List<String> values);
}
