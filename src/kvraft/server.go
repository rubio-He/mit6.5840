package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	PutOp    OpType = "Put"
	AppendOp OpType = "Append"
	GetOp    OpType = "Get"
)

type Op struct {
	Idx   int
	Type  OpType
	Key   string
	Value string

	Uuid int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	newestOpIdx  int
	kvmap        map[string]string
	opmap        map[int64]Op
	opResultChan chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("Failed, because not a leader")
		reply.Err = ErrWrongLeader
		return
	}
	// Skip the duplicate operations.
	if _, ok := kv.opmap[args.Uuid]; ok {
		reply.Err = OK
		reply.Value = kv.kvmap[args.Key]
		return
	}
	index := kv.newestOpIdx + 1
	op := Op{Idx: index, Type: GetOp, Key: args.Key, Value: "", Uuid: args.Uuid}
	DPrintf("Receive Get request, Send to Raft state machine.")
	kv.rf.Start(op) // TODO: need the term and index here.
	kv.wait(reply, GetOp, args.Uuid, index)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("Failed, because not a leader")
		reply.Err = ErrWrongLeader
		return
	}
	// Skip the duplicate operations.
	if _, ok := kv.opmap[args.Uuid]; ok {
		reply.Err = OK
		return
	}

	index := kv.newestOpIdx + 1
	op := Op{Idx: index, Type: PutOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid}
	DPrintf("Put op into raft state machine")
	kv.rf.Start(op)
	kv.wait(reply, PutOp, args.Uuid, index)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Skip the duplicate operations.
	if _, ok := kv.opmap[args.Uuid]; ok {
		reply.Err = OK
		return
	}
	_, ok := kv.kvmap[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return
	}

	index := kv.newestOpIdx + 1
	DPrintf("Append to the raft state machine")
	op := Op{Idx: index, Type: AppendOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid}
	kv.rf.Start(op)
	kv.wait(reply, AppendOp, args.Uuid, index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	DPrintf("Server built")
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.newestOpIdx = 0
	kv.opmap = make(map[int64]Op)
	kv.kvmap = make(map[string]string)
	kv.opResultChan = make(chan Op)
	go kv.applier()

	return kv
}

func (kv *KVServer) wait(reply interface{}, op OpType, uuid int64, index int) {
	for {
		select {
		case result := <-kv.opResultChan:
			switch result.Type {
			case GetOp:
				kv.opmap[result.Uuid] = result
			case PutOp:
				kv.opmap[result.Uuid] = result
				kv.kvmap[result.Key] = result.Value
			case AppendOp:
				kv.opmap[result.Uuid] = result
				kv.kvmap[result.Key] += result.Value
			}

			switch op {
			case GetOp:
				rply := reply.(*GetReply)
				if result.Uuid != uuid {
					rply.Err = ErrWrongLeader
					return
				}
				rply.Value = kv.kvmap[result.Key]
				rply.Err = OK
				return
			case PutOp, AppendOp:
				rply := reply.(*PutAppendReply)
				if result.Uuid != uuid {
					rply.Err = ErrWrongLeader
					return
				}
				rply.Err = OK
				return
			}

		default:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				switch op {
				case GetOp:
					rply := reply.(*GetReply)
					rply.Err = ErrWrongLeader
				case PutOp, AppendOp:
					rply := reply.(*PutAppendReply)
					rply.Err = ErrWrongLeader
				}
				return
			}
		}
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		op := msg.Command.(Op)
		if msg.CommandValid {
			switch op.Type {
			case PutOp:
			case AppendOp:
			case GetOp:
			}
			kv.opResultChan <- op
		}
	}
}
