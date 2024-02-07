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
	dedup  map[int64]int64
	kv     map[string]string
	kv_old map[Job]string
	mu     sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//DPrintf("Receive client GET request key %s from %d at uuid %d", args.Key, args.ClientId, args.Uuid)

	if val, ok := kv.kv[args.Key]; ok {
		reply.Value = val
	}
	DPrintf("After GET kv '%s'", kv.kv)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Receive client put request key %s value %s from %d at uuid %d", args.Key, args.Value, args.ClientId, args.Uuid)
	if uuid, ok := kv.dedup[args.ClientId]; ok {
		if uuid == args.Uuid {
			return
		}
	}
	kv.dedup[args.ClientId] = args.Uuid
	kv.kv[args.Key] = args.Value
	DPrintf("After put kv '%s'", kv.kv)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Receive client append request key %s value %s from %d at uuid %d", args.Key, args.Value, args.ClientId, args.Uuid)
	if uuid, ok := kv.dedup[args.ClientId]; ok {
		if uuid == args.Uuid {
			reply.Value = kv.kv_old[Job{args.Uuid, args.ClientId}]
			return
		}
	}

	kv.dedup[args.ClientId] = args.Uuid

	if val, ok := kv.kv[args.Key]; ok {
		kv.kv[args.Key] = val + args.Value
		kv.kv_old[Job{args.Uuid, args.ClientId}] = val
		reply.Value = val
	} else {
		kv.kv[args.Key] = args.Value
	}
	DPrintf("After put kv '%s'", kv.kv)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kv = make(map[string]string)
	kv.dedup = make(map[int64]int64)
	kv.kv_old = make(map[Job]string)

	return kv
}
