package ru.gitverse.bizzareowl.mapreduce.tasks;

import ru.gitverse.bizzareowl.mapreduce.KeyValue;
import java.util.List;

@FunctionalInterface
public interface MapTask {

    default MapTask defaultMap() {
        return (filename, content) -> {
          return null;
        };
    }

    List<KeyValue> map(String filename, String content);
}
