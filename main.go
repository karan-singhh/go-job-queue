package main

import (
	"fmt"
	"sync"
	"time"
)

type Task struct {
	ID        string
	Status    string
	Title     string
	Payload   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Result struct { // used to store only the output to be sent from workers
	TaskID    string
	Title     string
	Status    string
	WorkerID  int
	UpdatedAt time.Time
}

func main() {
	var wg sync.WaitGroup
	fmt.Println("job queue simulator program")

	taskA := &Task{
		ID:        "1",
		Status:    "accepted",
		Title:     "Task A",
		Payload:   "karan@gmail.com: Welcome to our service",
		CreatedAt: time.Now(),
		UpdatedAt: time.Time{},
	}
	taskB := &Task{
		ID:        "2",
		Status:    "accepted",
		Title:     "Task B",
		Payload:   "Rahul@gmail.com: Welcome to our service",
		CreatedAt: time.Now(),
		UpdatedAt: time.Time{},
	}
	taskC := &Task{
		ID:        "3",
		Status:    "accepted",
		Title:     "Task C",
		Payload:   "Fateh@gmail.com: Welcome to our service",
		CreatedAt: time.Now(),
		UpdatedAt: time.Time{},
	}
	taskD := &Task{
		ID:        "4",
		Status:    "accepted",
		Title:     "Task D",
		Payload:   "Anuj@gmail.com: Welcome to our service", // could be a payload with a huge attachment file to be sent via email
		CreatedAt: time.Now(),
		UpdatedAt: time.Time{},
	}
	taskE := &Task{
		ID:        "5",
		Status:    "accepted",
		Title:     "Task E",
		Payload:   "Babloo@gmail.com: Welcome to our service",
		CreatedAt: time.Now(),
		UpdatedAt: time.Time{},
	}
	taskF := &Task{
		ID:        "6",
		Status:    "accepted",
		Title:     "Task F",
		Payload:   "Lokesh@gmail.com: Welcome to our service",
		CreatedAt: time.Now(),
		UpdatedAt: time.Time{},
	}
	numTask := 6

	// updateStatus(taskA, "Completed")
	fmt.Println("before sending tasks to channel: \n", taskA, "\n", taskB, "\n", taskC, "\n", taskD, "\n", taskE, "\n", taskF)
	// fmt.Println("before task goes to worker- \n", taskA, "\n", taskB, "\n", taskC)
	// jobs := make(chan *Task) // creted an unbuffered channel
	jobs := make(chan *Task, 3)           // creted a buffered channel
	results := make(chan Result, numTask) // created buffered channel for workers to pass the result into
	// 1. LAUNCH WORKER SO IT STARTS LISTENING
	count := 3
	for i := 1; i <= count; i++ {

		wg.Add(1)
		go worker(jobs, results, &wg, i)
	}
	// 2. SEND TASK INTO THE CHANNEL
	jobs <- taskA
	jobs <- taskB
	jobs <- taskC
	jobs <- taskD
	jobs <- taskE
	jobs <- taskF
	// main is done sending tasks to the channel,close the channel to let the worker know that no more tasks are being sent via this channel
	close(jobs)

	// fmt.Println("tasks while worker processes in BG \n", taskA, "\n", taskB, "\n", taskC, "\n", taskD, "\n", taskE, "\n", taskF) // prints task status *WHILE* worker is processing it

	wg.Wait() //halts the main goroutine until the counter drops back down to zero
	// 1. Wait until all worker goroutines call wg.Done()
	// 2. Safely seal the results channel (no more sends will happen)
	close(results)
	for res := range results {
		fmt.Printf("\n Task %s - %s by worker %d ", res.TaskID, res.Status, res.WorkerID)
		fmt.Println("finally : ", res)
	}
	// fmt.Println("finally : \n", taskA, "\n", taskB, "\n", taskC, "\n", taskD, "\n", taskE, "\n", taskF)

}
func worker(jobs <-chan *Task, results chan<- Result, wg *sync.WaitGroup, wid int) {
	defer wg.Done() // decreases Counter by 1

	for t := range jobs {
		res := Result{
			TaskID: t.ID,
			Title:  t.Title,
			// Status:    t.Status,
			WorkerID: wid,
			// UpdatedAt: time.Time{},
		}
		// t := <-jobs // pulls a task from the channel

		fmt.Printf("worker %d started processing %s\n", wid, t.Title)
		time.Sleep(4 * time.Second) // for simulation
		res.Status = "Completed"    // actual work
		res.UpdatedAt = time.Now()
		fmt.Printf("worker %d completed  %s\n", wid, t.Title)
		results <- res
	}
}

// func updateStatus(myTask *Task, newStatus string) {
// 	myTask.Status = newStatus // Go does automatic dereferencing so no need to write *myTask.Status=
// 	// return not needed because already value has been changed in original memory address
// }
