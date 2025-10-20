package ru.gitverse.bizzareowl.mapreduce;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapReduceTests {

    @Test
    public void mapReduce_withSeveralTasksOfCountingWords_shouldWriteResultsToFile() throws IOException {

        Path taskDir = Files.createTempDirectory("tasks");
        Path intermediateResultPath = Files.createTempDirectory("inter");
        Path resultDir = Files.createTempDirectory("results");

        Path task1Path = taskDir.resolve("task1");
        Path task2Path = taskDir.resolve("task2");
        Path task3Path = taskDir.resolve("task3");

        Files.createFile(task1Path);
        Files.createFile(task2Path);
        Files.createFile(task3Path);

        Files.writeString(task1Path, "a b c d e f g h i j k l m n o p q r s t u v w x y z");
        Files.writeString(task2Path, "a b c d e f g h i j k");
        Files.writeString(task3Path, "a b c d e f");

        List<Path> tasksFiles = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(taskDir)) {
            directoryStream.forEach(tasksFiles::add);
        }

        Coordinator coordinator = new Coordinator(intermediateResultPath, resultDir);
        coordinator.mapAndReduce(tasksFiles, 10, 4, 2000,
                (_, content) -> Arrays.stream(content.split(" "))
                        .map(value -> new KeyValue(value.trim(), "1"))
                        .toList(),
                (_, values) -> values.stream()
                        .map(value -> value.split("=")[0])
                        .collect(Collectors.groupingBy(v -> v))
                        .entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue().size())
                        .collect(Collectors.joining("\n"))
        );

        List<Path> resultFiles = new ArrayList<>();
        List<String> resultReduced = new ArrayList<>();
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(resultDir)) {
            dirStream.forEach(path -> {
                resultFiles.add(path);
                try {
                    resultReduced.add(Files.readString(path));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        Map<String, Integer> groupedResults = resultReduced.stream()
                .flatMap(resultSet -> Arrays.stream(resultSet.split("\n")))
                .map(resultRow -> {
                    String[] split = resultRow.split("=");
                    return Map.entry(split[0], split[1]);
                })
                .collect(Collectors.groupingBy(Map.Entry::getKey))
                .entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().stream()
                        .mapToInt(e -> Integer.parseInt(e.getValue())).sum())
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


        Assertions.assertEquals(10, resultFiles.size());
        Assertions.assertEquals(3, groupedResults.get("a"));
        Assertions.assertEquals(3, groupedResults.get("b"));
        Assertions.assertEquals(3, groupedResults.get("c"));
        Assertions.assertEquals(3, groupedResults.get("d"));
        Assertions.assertEquals(3, groupedResults.get("e"));
        Assertions.assertEquals(3, groupedResults.get("f"));
        Assertions.assertEquals(2, groupedResults.get("g"));
        Assertions.assertEquals(2, groupedResults.get("h"));
        Assertions.assertEquals(2, groupedResults.get("i"));
        Assertions.assertEquals(2, groupedResults.get("j"));
        Assertions.assertEquals(2, groupedResults.get("k"));
        Assertions.assertEquals(1, groupedResults.get("l"));
        Assertions.assertEquals(1, groupedResults.get("m"));
        Assertions.assertEquals(1, groupedResults.get("n"));
        Assertions.assertEquals(1, groupedResults.get("o"));
        Assertions.assertEquals(1, groupedResults.get("p"));
        Assertions.assertEquals(1, groupedResults.get("q"));
        Assertions.assertEquals(1, groupedResults.get("r"));
        Assertions.assertEquals(1, groupedResults.get("s"));
        Assertions.assertEquals(1, groupedResults.get("t"));
        Assertions.assertEquals(1, groupedResults.get("u"));
        Assertions.assertEquals(1, groupedResults.get("v"));
        Assertions.assertEquals(1, groupedResults.get("w"));
        Assertions.assertEquals(1, groupedResults.get("x"));
        Assertions.assertEquals(1, groupedResults.get("y"));
        Assertions.assertEquals(1, groupedResults.get("z"));

    }

}
