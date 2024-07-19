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
	persister    *raft.Persister

	newestOpIdx  int
	kvmap        map[string]string
	opmap        map[int64]Op
	opResultChan chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Idx: kv.newestOpIdx + 1, Type: GetOp, Key: args.Key, Value: "", Uuid: args.Uuid}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Receive Get request, Send to Raft state machine.")
	kv.wait(reply, GetOp, args.Uuid, index)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Idx: kv.newestOpIdx + 1, Type: PutOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Put op %+v into raft state machine", args)
	kv.wait(reply, PutOp, args.Uuid, index)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Idx: kv.newestOpIdx + 1, Type: AppendOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("kv %d receive Append %+v", kv.me, args)
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
	kv.persister = persister
	kv.newestOpIdx = 0
	kv.opmap = make(map[int64]Op)
	kv.kvmap = make(map[string]string)
	kv.opResultChan = make(chan Op)

	//if persister.SnapshotSize() > 0 {
	//	//snapshot := persister.ReadSnapshot()
	//}
	return kv
}

func (kv *KVServer) wait(reply interface{}, op OpType, uuid int64, index int) {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				result := msg.Command.(Op)
				// Skip the duplicate operations.
				if _, ok := kv.opmap[result.Uuid]; !ok {
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
				}
				//if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				//	kv.rf.Snapshot()
				//}
				DPrintf("result op %+v", result)
				if result.Uuid != uuid {
					continue
				}
				switch op {
				case GetOp:
					rply := reply.(*GetReply)
					rply.Value = kv.kvmap[result.Key]
					rply.Err = OK
					return
				case PutOp, AppendOp:
					rply := reply.(*PutAppendReply)
					rply.Err = OK
					return
				}
			}
		default:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				DPrintf("kv %d not a leader anymore, return", kv.me)
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

//func (kv *KVServer) ReadSnapshot(snapshot []byte) interface{} {
//	r := bytes.NewBuffer(snapshot)
//	d := labgob.NewDecoder(r)
//}
