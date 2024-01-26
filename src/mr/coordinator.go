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

type Coordinator struct {
	files               []string
	fileIndex           int
	nReduce             int
	reduceTaskId        int
	completedMapTask    int
	completedReduceTask int
}

func (c *Coordinator) MapTask(_ *RpcArgs, response *MapTaskResponse) error {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	if c.fileIndex >= len(c.files) {
		response.Done = true
		return nil
	}
	response.TaskId = c.fileIndex
	response.File = c.files[c.fileIndex]
	response.ReduceCount = c.nReduce
	c.fileIndex++
	fmt.Printf("Sending file %s to worker", response.File)
	return nil
}

func (c *Coordinator) ReduceTask(_ *RpcArgs, response *ReduceTaskResponse) error {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	if c.completedMapTask != len(c.files) {
		return nil
	}
	if c.completedReduceTask >= c.nReduce {
		return nil
	}

	response.Ready = true
	fileNames := make([]string, c.completedMapTask)
	for i := 0; i < c.completedMapTask; i++ {
		fileNames[i] = fmt.Sprintf("mr-%d-%d.txt", i, c.reduceTaskId)
	}
	response.FileNames = fileNames
	response.TaskId = c.reduceTaskId
	c.reduceTaskId++
	return nil
}

func (c *Coordinator) Complete(args *TaskCompletionArgs, response *TaskCompletionResponse) error {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	if args.Type == Map {
		c.completedMapTask += 1
		fmt.Printf("Finish map task count: %d\n", c.completedMapTask)
	} else {
		c.completedReduceTask += 1
		fmt.Printf("Finish reduce task count: %d\n", c.completedReduceTask)
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
	if c.completedReduceTask >= c.nReduce {
		return true
	}
	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, 0, nReduce, 0, 0, 0}
	c.server()
	return &c
}
