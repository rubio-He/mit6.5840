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
	// Early return if the server is killed.
	if kv.killed() {
		reply.Err = ErrKilled
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Idx: kv.newestOpIdx + 1, Type: GetOp, Key: args.Key, Value: ""}
	kv.rf.Start(op) // TODO: need the term and index here.
	kv.mu.Unlock()
	op = <-kv.opResultChan
	kv.mu.Lock()
	DPrintf("%+v", kv.kvmap)
	reply.Value = op.Value
	reply.Err = OK
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrKilled
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// Skip the duplicate operations.
	if _, ok := kv.opmap[args.Uuid]; !ok {
		kv.kvmap[args.Key] = args.Value
		op := Op{Idx: kv.newestOpIdx + 1, Type: PutOp, Key: args.Key, Value: args.Value}
		kv.opmap[args.Uuid] = op

		kv.rf.Start(op)

		kv.mu.Unlock()
		<-kv.opResultChan
		kv.mu.Lock()
	}
	reply.Err = OK
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrKilled
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Skip the duplicate operations.
	if _, ok := kv.opmap[args.Uuid]; !ok {
		if _, ok := kv.kvmap[args.Key]; ok {
			op := Op{Idx: kv.newestOpIdx + 1, Type: AppendOp, Key: args.Key, Value: args.Value}
			kv.opmap[args.Uuid] = op
			kv.rf.Start(op)
			kv.mu.Unlock()
			<-kv.opResultChan
			kv.mu.Lock()
		}
	}
	reply.Err = OK
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

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		op := msg.Command.(Op)
		if msg.CommandValid {
			switch op.Type {
			case PutOp:
				kv.kvmap[op.Key] = op.Value
			case AppendOp:
				kv.kvmap[op.Key] += op.Value
			case GetOp:
				if val, ok := kv.kvmap[op.Key]; ok {
					op.Value = val
				}
			}
			kv.newestOpIdx += 1
		}
		kv.mu.Unlock()
		kv.opResultChan <- op
	}
}
