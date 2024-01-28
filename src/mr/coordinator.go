package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Status int

const (
	TODO Status = iota
	PENDING
	DONE
)

type Coordinator struct {
	files            []string
	nReduce          int
	MapTaskStatus    []Status
	ReduceTaskStatus []Status
}

func (c *Coordinator) MapTask(_ *RpcArgs, response *MapTaskResponse) error {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	for i, v := range c.MapTaskStatus {
		if v == DONE || v == PENDING {
			continue
		}
		response.TaskId = i
		response.File = c.files[i]
		response.ReduceCount = c.nReduce
		fmt.Printf("Sending file %s to worker", response.File)
		return nil
	}
	response.Done = true
	return nil
}

func (c *Coordinator) ReduceTask(_ *RpcArgs, response *ReduceTaskResponse) error {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	if countCompletedTask(c.MapTaskStatus) != len(c.files) {
		return nil
	}
	if countCompletedTask(c.ReduceTaskStatus) >= c.nReduce {
		return nil
	}
	for i, v := range c.ReduceTaskStatus {
		if v == DONE || v == PENDING {
			continue
		}
		response.Ready = true
		tasks := countCompletedTask(c.MapTaskStatus)
		fileNames := make([]string, tasks)
		for i := 0; i < tasks; i++ {
			fileNames[i] = fmt.Sprintf("mr-%d-%d.txt", i, i)
		}
		response.FileNames = fileNames
		response.TaskId = i
	}

	return nil
}

func (c *Coordinator) Complete(args *TaskCompletionArgs, response *TaskCompletionResponse) error {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	if args.Type == Map {
		c.MapTaskStatus[args.Id] = DONE
		fmt.Printf("Finish map task count: %d\n", c.MapTaskStatus)
	} else {
		c.ReduceTaskStatus[args.Id] = DONE
		fmt.Printf("Finish reduce task count: %d\n", c.ReduceTaskStatus)
	}

	return nil
}

// server start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if countCompletedTask(c.ReduceTaskStatus) >= c.nReduce {
		return true
	}
	return false
}

func countCompletedTask(tasks []Status) int {
	countOfCompletedTask := 0
	for _, v := range tasks {
		if v == DONE {
			countOfCompletedTask += 1
		}
	}
	return countOfCompletedTask
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, nReduce, make([]Status, len(files)), make([]Status, nReduce)}
	c.server()
	return &c
}
