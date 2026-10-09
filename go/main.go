
package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// Task represents a ride processing request.
type Task struct {
	ID    int
	Route string
}

// Result stores the processed ride information.
type Result struct {
	ID      int
	Message string
}

// SharedQueue manages tasks using a Go channel.
type SharedQueue struct {
	tasks chan Task
}

func NewSharedQueue(size int) *SharedQueue {
	return &SharedQueue{
		tasks: make(chan Task, size),
	}
}

func (q *SharedQueue) addTask(task Task) {
	q.tasks <- task
}

func (q *SharedQueue) getTask() (Task, bool) {
	task, ok := <-q.tasks
	return task, ok
}

func (q *SharedQueue) close() {
	close(q.tasks)
}

// SharedResults protects the results using a mutex.
type SharedResults struct {
	mu      sync.Mutex
	results []Result
}

func (s *SharedResults) addResult(result Result) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.results = append(s.results, result)
}

func (s *SharedResults) getResults() []Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyResults := make([]Result, len(s.results))
	copy(copyResults, s.results)

	return copyResults
}

// Worker processes tasks concurrently.
func worker(
	id int,
	queue *SharedQueue,
	results *SharedResults,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	workerName := fmt.Sprintf("Worker-%d", id)
	log.Printf("%s started.", workerName)

	for {
		task, ok := queue.getTask()

		if !ok {
			break
		}

		rideName := fmt.Sprintf("Ride-%03d: %s",
			task.ID, task.Route)

		log.Printf("%s processing %s",
			workerName, rideName)

		// Simulate computational processing.
		time.Sleep(500 * time.Millisecond)

		message := fmt.Sprintf(
			"%s processed successfully by %s",
			rideName, workerName,
		)

		results.addResult(Result{
			ID:      task.ID,
			Message: message,
		})

		log.Printf("%s completed %s",
			workerName, rideName)
	}

	log.Printf("%s finished.", workerName)
}

// Save processed results to a file.
func saveResults(filename string, results []Result) error {
	file, err := os.Create(filename)

	if err != nil {
		return fmt.Errorf("cannot create file: %w", err)
	}

	defer file.Close()

	for _, result := range results {
		_, err := fmt.Fprintln(file, result.Message)

		if err != nil {
			return fmt.Errorf("cannot write result: %w", err)
		}
	}

	return nil
}

func main() {
	fmt.Println("=== Multi-threaded Ride Data Processing System (Go) ===")

	tasks := []Task{
		{1, "Reston -> Herndon"},
		{2, "Ashburn -> Reston"},
		{3, "Herndon -> Sterling"},
		{4, "Sterling -> Ashburn"},
		{5, "Reston -> Ashburn"},
		{6, "Herndon -> Reston"},
		{7, "Ashburn -> Sterling"},
		{8, "Sterling -> Herndon"},
		{9, "Reston -> Sterling"},
		{10, "Ashburn -> Herndon"},
	}

	queue := NewSharedQueue(len(tasks))
	results := &SharedResults{}

	var wg sync.WaitGroup

	// Add tasks to the shared queue.
	for _, task := range tasks {
		queue.addTask(task)
	}

	queue.close()

	// Start three worker goroutines.
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker(i, queue, results, &wg)
	}

	// Wait until all workers finish.
	wg.Wait()

	processedResults := results.getResults()

	// Sort output by ride ID for readability.
	sort.Slice(processedResults, func(i, j int) bool {
		return processedResults[i].ID < processedResults[j].ID
	})

	fmt.Println("\n=== Processing Results ===")

	for _, result := range processedResults {
		fmt.Println(result.Message)
	}

	fmt.Printf("\nTotal tasks processed: %d\n",
		len(processedResults))

	if len(processedResults) != len(tasks) {
		log.Printf(
			"ERROR: Expected %d tasks but processed %d",
			len(tasks), len(processedResults),
		)
		return
	}

	// Save results to a text file.
	err := saveResults(
		"processing_results.txt",
		processedResults,
	)

	if err != nil {
		log.Printf("ERROR saving results: %v", err)
		return
	}

	fmt.Println("Results saved successfully to processing_results.txt.")
	fmt.Println("System completed successfully.")
}
