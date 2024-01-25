package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reduce func(string, []string) string) {
	for {
		mapRpcResponse := MapTaskResponse{Done: false}
		ok := call("Coordinator.MapTask", &MapTaskArgs{}, &mapRpcResponse)
		if !ok {
			os.Exit(1)
		}
		if mapRpcResponse.Done {
			break
		}
		mapJob(mapf, &mapRpcResponse)
		completionRpcArgs := TaskCompletionArgs{Map}
		ok = call("Coordinator.Complete", &completionRpcArgs, &TaskCompletionResponse{})
		if !ok {
			os.Exit(1)
		}
		time.Sleep(1 * time.Second)
	}

}

func mapJob(mapf func(string, string) []KeyValue, response *MapTaskResponse) {
	filename := response.File
	fmt.Printf("Worker received Map Task: %d, reduce number: %d, %s\n",
		response.TaskId, response.ReduceCount, filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", filename)
	}
	defer file.Close()

	// Write to result the temp file.
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % response.ReduceCount
		tempFileName := fmt.Sprintf("mr-%d-%d.txt", response.TaskId, reduceId)
		temp, err := os.OpenFile(tempFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(temp)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Encoding error: %s", err)
		}
	}
}

// call send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
