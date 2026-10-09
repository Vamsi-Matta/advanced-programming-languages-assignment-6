# advanced-programming-languages-assignment-6

# Assignment 6: Multi-threaded Data Processing System

**Author:** Vamsi Matta  
**Course:** Advanced Programming Languages  
**University:** University of the Cumberlands

## Overview

This project implements the same concurrent ride-request processing workload in **Java** and **Go**. Each program distributes 10 ride tasks among three workers, simulates processing time, records results safely, and writes a `processing_results.txt` file.

## Project structure

```text
advanced-programming-languages-assignment-6/
├── java/
│   └── DataProcessingSystem.java
├── go/
│   └── main.go
├── screenshots/
│   ├── java_output.png
│   └── go_output.png
├── README.md
└── .gitignore
```

## Java implementation

- Synchronized `addTask()` and `getTask()` methods protect a shared queue.
- `ExecutorService` runs three workers.
- A synchronized results list collects processed ride requests.
- `Thread.sleep()` simulates work; `try-catch-finally` handles interruption and reports errors.
- `awaitTermination()` waits for workers, and `FileWriter` saves the output with `IOException` handling.

### Run Java

From the repository root:

```powershell
cd java
javac DataProcessingSystem.java
java DataProcessingSystem
```

## Go implementation

- A channel provides the shared task queue (`addTask()` / `getTask()`).
- Three goroutines process tasks concurrently.
- `sync.WaitGroup` ensures workers finish; `sync.Mutex` protects the results slice.
- `time.Sleep()` simulates work; returned errors and `defer` manage file operations and worker cleanup.
- Results are sorted by ride ID for readable output.

### Run Go

From the repository root:

```powershell
cd go
go run main.go
```

## Observed sample results

Both programs were run successfully with **10 tasks and three workers**. Each run displayed worker start, processing, completion, and finish messages, followed by:

```text
Total tasks processed: 10
Results saved successfully to processing_results.txt.
System completed successfully.
```

Worker assignment and log order can vary between runs because execution is concurrent. The sample runs show no missing or duplicate ride IDs in their result lists.

## Screenshots

The `screenshots/` folder contains console-output evidence for both implementations. Screenshots should show the key synchronization mechanisms and the successful processing summary.

## Notes

- `processing_results.txt` is generated in the working directory where the program is run.
- Java `.class` files and generated results are excluded from Git by `.gitignore`.
- Successful demonstration runs do not by themselves prove freedom from all possible race conditions or failure modes.

## References

- Go Authors. (n.d.). *Effective Go*. https://go.dev/doc/effective_go
- Oracle. (n.d.). *Concurrency*. The Java Tutorials. https://docs.oracle.com/javase/tutorial/essential/concurrency/
