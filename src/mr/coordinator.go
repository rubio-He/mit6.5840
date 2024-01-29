package mr

import (
	"6.5840/util"
	"fmt"
	"log"
	"sync"
	"time"
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
	mu               sync.Mutex
	files            []string
	nReduce          int
	MapTaskStatus    []Status
	ReduceTaskStatus []Status
	MapTaskTime      []time.Time
	ReduceTaskTime   []time.Time
}

func (c *Coordinator) MapTask(_ *RpcArgs, response *MapTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, v := range c.MapTaskStatus {
		if v == DONE {
			continue
		} else if v == PENDING && time.Now().Sub(c.MapTaskTime[i]) < 5*time.Second {
			continue
		}
		c.MapTaskStatus[i] = PENDING
		response.TaskId = i
		response.File = c.files[i]
		response.ReduceCount = c.nReduce
		c.MapTaskTime[i] = time.Now()
		util.Println("Sending file %s to worker", response.File)
		return nil
	}
	response.Done = true
	return nil
}

func (c *Coordinator) ReduceTask(_ *RpcArgs, response *ReduceTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if countCompletedTask(c.MapTaskStatus) != len(c.files) {
		return nil
	}
	if countCompletedTask(c.ReduceTaskStatus) >= c.nReduce {
		return nil
	}
	for i, v := range c.ReduceTaskStatus {
		if v == DONE {
			continue
		} else if v == PENDING && time.Now().Sub(c.ReduceTaskTime[i]) < time.Second*3 {
			continue
		}
		c.ReduceTaskStatus[i] = PENDING
		response.Ready = true
		tasks := len(c.files)
		fileNames := make([]string, tasks)
		for j := 0; j < tasks; j++ {
			fileNames[j] = fmt.Sprintf("mr-%d-%d.txt", j, i)
		}
		response.FileNames = fileNames
		response.TaskId = i
		c.ReduceTaskTime[i] = time.Now()
		util.Println("Sending reduce task %d to worker", i)
		return nil
	}

	return nil
}

func (c *Coordinator) Complete(args *TaskCompletionArgs, response *TaskCompletionResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == Map {
		c.MapTaskStatus[args.Id] = DONE
		util.Println("Finish map task count: %d", c.MapTaskStatus)
	} else {
		c.ReduceTaskStatus[args.Id] = DONE
		util.Println("Finish reduce task count: %d", c.ReduceTaskStatus)
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
	c := Coordinator{
		files:            files,
		nReduce:          nReduce,
		MapTaskStatus:    make([]Status, len(files)),
		ReduceTaskStatus: make([]Status, nReduce),
		MapTaskTime:      make([]time.Time, len(files)),
		ReduceTaskTime:   make([]time.Time, nReduce)}
	c.server()
	return &c
}
