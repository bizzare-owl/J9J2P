package ru.gitverse.bizzareowl.mapreduce;

import ru.gitverse.bizzareowl.mapreduce.tasks.MapTask;
import ru.gitverse.bizzareowl.mapreduce.tasks.ReduceTask;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public abstract class Worker {

    private final MapTask map;
    private final ReduceTask reduce;

    protected Worker(MapTask map, ReduceTask reduce) {
        this.map = map;
        this.reduce = reduce;
    }

    private static class WorkerImpl extends Worker {
        protected WorkerImpl(MapTask map, ReduceTask reduce) {
            super(map, reduce);
        }
    }

    public static Worker of(MapTask map, ReduceTask reduce) {
        return new WorkerImpl(map, reduce);
    }

    public boolean map(Path filePath, Path resultStoragePath, int taskId, int reduceTasksCount) {
        try {
            System.out.println("Starting performing mapping operations");
            map.map(filePath.getFileName().toString(), Files.readString(filePath))
                    .stream()
                    .collect(Collectors.groupingBy(keyValue -> keyValue.key().hashCode() % reduceTasksCount))
                    .forEach((partition, keyValues) -> {
                        try {
                            System.out.println("Starting writing mr-files");
                            Files.writeString(
                                    resultStoragePath.resolve(String.format("mr-%d-%d", taskId, partition)),
                                    keyValues.stream().sorted(Comparator.comparing(KeyValue::key)).map(KeyValue::toString).collect(Collectors.joining("\n")),
                                    StandardOpenOption.CREATE_NEW
                            );
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            return true;

        } catch (IOException e) {
            return false;
        }
    }

    public boolean reduce(Path intermediateResultStoragePath, Path resultStoragePath, int partitionKey) {
        try (DirectoryStream<Path> intermediateResultFiles = Files.newDirectoryStream(intermediateResultStoragePath)) {
            final List<String> values = new ArrayList<>();
            intermediateResultFiles.forEach(path -> {
                    if (path.getFileName().toString().matches("mr-[0-9]+-" + partitionKey)) {
                        try {
                            Arrays.stream(Files.readString(path).split("\n"))
                                    .filter(value -> !value.isBlank())
                                    .forEach(value -> values.add(value.split("=")[0]));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            );

            String reduceResult = reduce.reduce(Integer.toString(partitionKey), values.stream().filter(value -> !value.isBlank()).toList());
            Files.writeString(resultStoragePath.resolve("r-" + partitionKey), reduceResult, StandardOpenOption.CREATE_NEW);
            return true;

        } catch (IOException e) {
            return false;
        }
    }
}
