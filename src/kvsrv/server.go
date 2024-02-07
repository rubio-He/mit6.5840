package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Job struct {
	id int64
	c  int64
}

type KVServer struct {
	pendingOp   map[int64]int64 // Mapping from client id to op id
	kv          map[string]string
	pendingOldV map[Job]string // Pending appending op's value
	mu          sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.kv[args.Key]; ok {
		reply.Value = val
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if uuid, ok := kv.pendingOp[args.ClientId]; ok {
		if uuid == args.Uuid {
			return
		}
	}
	kv.pendingOp[args.ClientId] = args.Uuid
	kv.kv[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if uuid, ok := kv.pendingOp[args.ClientId]; ok {
		if uuid == args.Uuid {
			reply.Value = kv.pendingOldV[Job{args.Uuid, args.ClientId}]
			return
		}
	}

	// Remove pending op's value.
	oldId := kv.pendingOp[args.ClientId]
	delete(kv.pendingOldV, Job{oldId, args.ClientId})

	// Update pending op's id
	kv.pendingOp[args.ClientId] = args.Uuid

	if val, ok := kv.kv[args.Key]; ok {
		kv.kv[args.Key] = val + args.Value
		kv.pendingOldV[Job{args.Uuid, args.ClientId}] = val
		reply.Value = val
	} else {
		kv.kv[args.Key] = args.Value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kv = make(map[string]string)
	kv.pendingOp = make(map[int64]int64)
	kv.pendingOldV = make(map[Job]string)

	return kv
}
