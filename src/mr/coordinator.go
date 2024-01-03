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
	files   []string
	index   int
	nReduce int
}

func (c *Coordinator) MapTask(args *MapTaskArgs, response *MapTaskResponse) error {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	response.File = c.files[c.index]
	c.index++
	fmt.Printf("Sending file %s to worker", response.File)
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
	//if c.index == len(c.files) {
	//	return true
	//}
	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, 0, nReduce}
	fmt.Println(files)
	c.server()
	return &c
}
