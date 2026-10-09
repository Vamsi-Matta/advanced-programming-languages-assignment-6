import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataProcessingSystem {

    // Shared task queue
    private static final Queue<String> taskQueue = new LinkedList<>();

    // Shared results list
    private static final List<String> results =
            Collections.synchronizedList(new ArrayList<>());

    // Safely retrieve a task from the shared queue
    public static synchronized String getTask() {
        if (taskQueue.isEmpty()) {
            return null;
        }
        return taskQueue.poll();
    }

    // Safely add a task to the shared queue
    public static synchronized void addTask(String task) {
        taskQueue.offer(task);
    }

    // Worker class
    static class Worker implements Runnable {

        private final int workerId;

        public Worker(int workerId) {
            this.workerId = workerId;
        }

        @Override
        public void run() {
            String workerName = "Worker-" + workerId;
            System.out.println(workerName + " started.");

            try {
                while (true) {
                    String task = getTask();

                    if (task == null) {
                        break;
                    }

                    System.out.println(workerName + " processing " + task);

                    // Simulate computational work
                    Thread.sleep(500);

                    String result =
                            task + " processed successfully by " + workerName;

                    results.add(result);

                    System.out.println(workerName + " completed " + task);
                }
            } catch (InterruptedException e) {
                System.err.println(
                        workerName + " interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println(
                        workerName + " encountered an error: " + e.getMessage());
            } finally {
                System.out.println(workerName + " finished.");
            }
        }
    }

    public static void main(String[] args) {

        System.out.println(
                "=== Multi-threaded Ride Data Processing System ===");

        // Add ride-processing tasks
        addTask("Ride-001: Reston -> Herndon");
        addTask("Ride-002: Ashburn -> Reston");
        addTask("Ride-003: Herndon -> Sterling");
        addTask("Ride-004: Sterling -> Ashburn");
        addTask("Ride-005: Reston -> Ashburn");
        addTask("Ride-006: Herndon -> Reston");
        addTask("Ride-007: Ashburn -> Sterling");
        addTask("Ride-008: Sterling -> Herndon");
        addTask("Ride-009: Reston -> Sterling");
        addTask("Ride-010: Ashburn -> Herndon");

        // Create three worker threads
        ExecutorService executor = Executors.newFixedThreadPool(3);

        for (int i = 1; i <= 3; i++) {
            executor.execute(new Worker(i));
        }

        // Stop accepting new workers
        executor.shutdown();

        try {
            // Wait for all workers to finish
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println(
                        "Workers did not finish within the expected time.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println(
                    "Main thread interrupted: " + e.getMessage());
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("\n=== Processing Results ===");

        synchronized (results) {
            for (String result : results) {
                System.out.println(result);
            }
        }

        System.out.println("\nTotal tasks processed: " + results.size());

        // Save results to a shared output file
        try (FileWriter writer = new FileWriter("processing_results.txt")) {

            synchronized (results) {
                for (String result : results) {
                    writer.write(result + System.lineSeparator());
                }
            }

            System.out.println(
                    "Results saved successfully to processing_results.txt.");

        } catch (IOException e) {
            System.err.println(
                    "Error writing results file: " + e.getMessage());
        }

        System.out.println("System completed successfully.");
    }
}