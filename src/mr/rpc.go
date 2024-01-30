package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	Empty TaskType = iota
	Map
	Reduce
)

type JobArgs struct {
	WorkerId int
}

type JobResponse struct {
	TaskType    TaskType
	TaskId      int
	ReduceCount int
	MapCount    int
	File        string
}

type TaskCompletionArgs struct {
	Type TaskType
	Id   int
}

type TaskCompletionResponse struct {
}

type ReduceTaskResponse struct {
	Ready     bool
	TaskId    int
	FileNames []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
