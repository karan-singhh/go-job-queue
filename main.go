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
	}
	taskB := &Task{
		ID:        "2",
		Status:    "accepted",
		Title:     "Task B",
		Payload:   "Rahul@gmail.com: Welcome to our service",
		CreatedAt: time.Now(),
	}
	taskC := &Task{
		ID:        "3",
		Status:    "accepted",
		Title:     "Task C",
		Payload:   "Fateh@gmail.com: Welcome to our service",
		CreatedAt: time.Now(),
	}

	// updateStatus(taskA, "Completed")
	fmt.Println("before task goes to worker- \n", taskA, "\n", taskB, "\n", taskC)

	jobs := make(chan *Task) // creted an unbuffered channel

	// 1. LAUNCH WORKER SO IT STARTS LISTENING

	wg.Add(1)
	go worker(jobs, &wg)
	// 2. SEND TASK INTO THE CHANNEL
	jobs <- taskA
	jobs <- taskB
	jobs <- taskC
	// main is done sending tasks to the channel,close the channel to let the worker know that no more tasks are being sent via this channel
	close(jobs)

	fmt.Println("tasks while worker processes in BG \n", taskA, "\n", taskB, "\n", taskC) // prints task status *WHILE* worker is processing it

	// time.Sleep(4 * time.Second)
	wg.Wait() //halts the main goroutine until the counter drops back down to zero
	fmt.Println("finally task is", taskA, "\n", taskB, "\n", taskC)
	// go worker(&taskA)

}
func worker(jobs chan *Task, wg *sync.WaitGroup) {
	defer wg.Done() // decreases Counter by 1
	for t := range jobs {

		// t := <-jobs // pulls a task from the channel
		t.Status = "processing"
		time.Sleep(4 * time.Second) // for simulation
		t.Status = "Completed"      // actual work
	}
}

// func updateStatus(myTask *Task, newStatus string) {
// 	myTask.Status = newStatus // Go does automatic dereferencing so no need to write *myTask.Status=
// 	// return not needed because already value has been changed in original memory address
// }
