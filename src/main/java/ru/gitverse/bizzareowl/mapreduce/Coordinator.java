package ru.gitverse.bizzareowl.mapreduce;

import ru.gitverse.bizzareowl.mapreduce.tasks.MapTask;
import ru.gitverse.bizzareowl.mapreduce.tasks.ReduceTask;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public final class Coordinator {
    // Заменить на потокобезопасные
    private final Queue<Worker> freeWorkers = new LinkedList<>();
    private final Path taskIntermediateResultStoragePath;
    private final Path taskFinalResultStoragePath;
    private int nextTaskId = 0;

    public Coordinator(Path taskIntermediateResultStoragePath, Path taskFinalResultStoragePath) {
        this.taskIntermediateResultStoragePath = taskIntermediateResultStoragePath;
        this.taskFinalResultStoragePath = taskFinalResultStoragePath;
    }

    public Coordinator(String taskIntermediateResultStoragePath, String taskFinalResultStoragePath) {
        this(Paths.get(taskIntermediateResultStoragePath), Paths.get(taskFinalResultStoragePath));
    }

    public void mapAndReduce(String taskFileName, int reduceTasksCount, MapTask mapTask, ReduceTask reduceTask) {
        mapAndReduce(new String[]{taskFileName}, reduceTasksCount, reduceTasksCount, 2000, mapTask, reduceTask);
    }

    public void mapAndReduce(String taskFileName, int reduceTasksCount, int workersCount, int timeout, MapTask mapTask, ReduceTask reduceTask) {
        mapAndReduce(new String[]{taskFileName}, reduceTasksCount, workersCount, timeout, mapTask, reduceTask);
    }

    public void mapAndReduce(String[] taskFileNames, int reduceTasksCount, MapTask mapTask, ReduceTask reduceTask) {
        mapAndReduce(taskFileNames, reduceTasksCount, reduceTasksCount, 2000, mapTask, reduceTask);
    }

    public void mapAndReduce(String[] taskFileNames, int reduceTasksCount, int workersCount, int timeout, MapTask mapTask, ReduceTask reduceTask) {
        mapAndReduce(Arrays.stream(taskFileNames).map(Paths::get).toList(), reduceTasksCount, workersCount, timeout, mapTask, reduceTask);
    }

    public void mapAndReduce(List<Path> taskFilePaths, int reduceTasksCount, MapTask mapTask, ReduceTask reduceTask) {
        mapAndReduce(taskFilePaths, reduceTasksCount, reduceTasksCount, 2000, mapTask, reduceTask);
    }

    public void mapAndReduce(List<Path> taskFilePaths, int reduceTasksCount, int workersCount, int timeout, MapTask mapTask, ReduceTask reduceTask) {
        if (Objects.requireNonNull(taskFilePaths).isEmpty())
            throw new IllegalArgumentException("Tasks filepaths cannot be empty");
        if (reduceTasksCount <= 0)
            throw new IllegalArgumentException("Count of reduce tasks cannot be less than 1");
        if (workersCount <= 0)
            throw new IllegalArgumentException("Workers count cannot be less than 1");
        if (timeout <= 0)
            throw new IllegalArgumentException("Timeout cannot be less then zero");
        if (mapTask == null)
            throw new IllegalArgumentException("MapTask cannot be null");
        if (reduceTask == null)
            throw new IllegalArgumentException("ReduceTask cannot be null");

        try {
            clearStorages();

            System.out.println("Initialization of workers");
            freeWorkers.addAll(IntStream.range(0, workersCount).mapToObj(_ -> Worker.of(mapTask, reduceTask)).toList());

            System.out.println("Starting mapping...");
            map(taskFilePaths, reduceTasksCount, timeout);
            reduce(reduceTasksCount, timeout);

            clearStorage(taskIntermediateResultStoragePath);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private void clearStorages() throws IOException {
        clearStorage(taskIntermediateResultStoragePath);
        clearStorage(taskFinalResultStoragePath);
    }

    private void clearStorage(Path storagePath) throws IOException {
        try (DirectoryStream<Path> storage = Files.newDirectoryStream(storagePath)) {
            for (Path file : storage) {
                Files.deleteIfExists(file);
            }
        }
    }

    private void map(List<Path> taskFilePaths, int reduceTasksCount, int timeout) {
        final List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        final Queue<Path> mapTasks = new ConcurrentLinkedQueue<>(taskFilePaths);
        while (!mapTasks.isEmpty()) {

            Worker worker = freeWorkers.poll();
            if (worker == null) {
                continue;
            }

            Path taskToExecute = mapTasks.poll();
            if (taskToExecute == null) {
                System.out.println("There are now available mapping tasks. Return worker on waiting");
                freeWorkers.add(worker);
                continue;
            }

            int currentTaskId = nextTaskId++;
            System.out.println("Creating new map task:\t" + currentTaskId);
            CompletableFuture<Boolean> currentTaskFuture = CompletableFuture
                    .supplyAsync(() -> worker.map(taskToExecute, taskIntermediateResultStoragePath, currentTaskId, reduceTasksCount))
                    .completeOnTimeout(false, timeout, TimeUnit.MILLISECONDS)
                    .handle((result, ex) -> {
                        if (result == null || !result || ex != null) {
                            System.out.println("Error while perform mapping task");
                            mapTasks.add(taskToExecute);
                            freeWorkers.add(worker);
                            System.out.println("Trying to delete pollute files from intermediate result's storage");
                            try (DirectoryStream<Path> filesInStorage = Files.newDirectoryStream(taskIntermediateResultStoragePath)) {
                                for (Path path : filesInStorage) {
                                    if (path.getFileName().toString().matches(String.format("mr-%d-[0-9]+", currentTaskId))) {
                                        System.out.println("Delete file:\t" + path.getFileName());
                                        Files.delete(path);
                                    }
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            freeWorkers.add(worker);
                        }

                        return result;
                    });

            System.out.println("Added task to complete");
            futures.add(currentTaskFuture);
        }

        System.out.println("Waiting for all map tasks completing");
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((_, _) -> System.out.println("Complete")).join();
    }

    private void reduce(int reduceTasksCount, int timeout) {
        final List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        final Queue<Integer> reduceTasks = new ConcurrentLinkedQueue<>(IntStream.range(0, reduceTasksCount).boxed().toList());
        System.out.println(freeWorkers);
        while (!reduceTasks.isEmpty()) {

            Worker worker = freeWorkers.poll();
            if (worker == null) {
                continue;
            }

            Integer taskToExecute = reduceTasks.poll();
            System.out.println("Creating new reduce task:\t" + taskToExecute);
            if (taskToExecute == null) {
                System.out.println("Tasks to reduce are completed ");
                freeWorkers.add(worker);
                continue;
            }

            CompletableFuture<Boolean> currentFuture = CompletableFuture
                    .supplyAsync(() -> worker.reduce(taskIntermediateResultStoragePath, taskFinalResultStoragePath, taskToExecute))
                    .completeOnTimeout(false, timeout, TimeUnit.MILLISECONDS)
                    .handle((result, ex) -> {
                        if (result == false || ex != null) {
                            try {
                                reduceTasks.add(taskToExecute);
                                freeWorkers.add(worker);
                                System.out.println("Trying to delete pollute files from intermediate result's storage");
                                Files.deleteIfExists(taskFinalResultStoragePath.resolve("r-" + taskToExecute));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            freeWorkers.add(worker);
                        }

                        return false;
                    });

            futures.add(currentFuture);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
}
