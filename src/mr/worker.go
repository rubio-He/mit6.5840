package mr

import (
	"6.5840/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		ok := call("Coordinator.MapTask", &RpcArgs{}, &mapRpcResponse)
		if !ok {
			os.Exit(1)
		}
		if mapRpcResponse.Done {
			break
		}
		mapJob(mapf, &mapRpcResponse)
		ok = call("Coordinator.Complete", &TaskCompletionArgs{Map, mapRpcResponse.TaskId}, &TaskCompletionResponse{})
		if !ok {
			log.Fatal("Failed to finish the map task.")
		}
		time.Sleep(time.Second)
	}

	for {
		reduceRpcResponse := ReduceTaskResponse{Ready: false}
		ok := call("Coordinator.ReduceTask", &RpcArgs{}, &reduceRpcResponse)
		if !ok {
			log.Printf("Map reduce job finished!")
			break
		}
		if !reduceRpcResponse.Ready {
			continue
		}
		reduceJob(reduce, &reduceRpcResponse)
		ok = call("Coordinator.Complete", &TaskCompletionArgs{Reduce, reduceRpcResponse.TaskId}, &TaskCompletionResponse{})
		if !ok {
			log.Fatal("Failed to finish the reduce task.")
		}
		time.Sleep(time.Second)
	}

}

func mapJob(mapf func(string, string) []KeyValue, response *MapTaskResponse) {
	filename := response.File
	util.Println("Worker %d received Map Task: %d, reduce number: %d, %s",
		os.Getpid(), response.TaskId, response.ReduceCount, filename)
	file, err := os.Open(filename)
	if err != nil {
		util.Println("cannot open %v", filename)
		os.Exit(1)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		util.Println("cannot read %v", filename)
		os.Exit(1)
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
			util.Println("Encoding error: %s", err)
			os.Exit(1)
		}
		temp.Close()
	}
}

func reduceJob(reducef func(string, []string) string, response *ReduceTaskResponse) {
	fileNames := response.FileNames

	kva := []KeyValue{}
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			util.Println("ReduceJob %d Failed to open file %s", response.TaskId, fileName)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		os.Remove(fileName)
	}

	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", response.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	ofile.Close()
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

	fmt.Println("Coordinator has gone.")
	return false
}
