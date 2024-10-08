package kvraft

import (
	"bytes"
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
	Type  OpType
	Key   string
	Value string

	Uuid     int64
	ClientId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	pendingOp map[int]int64
	kvmap     map[string]string
	opmap     map[int64]Op
	opChan    map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	op := Op{Type: GetOp, Key: args.Key, Value: "", Uuid: args.Uuid, ClientId: args.ClientId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.opChan[index]; !ok {
		kv.opChan[index] = make(chan Op)
	}
	DPrintf("kv %d receive Get %+v at index %d", kv.me, args, index)
	opChan := kv.opChan[index]

	if id, ok := kv.pendingOp[args.ClientId]; ok {
		if id != args.Uuid {
			delete(kv.opmap, id)
		}
	}
	kv.pendingOp[args.ClientId] = args.Uuid

	kv.mu.Unlock()

	kv.waitGet(args, reply, opChan)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.opChan, index)
}

func (kv *KVServer) waitGet(args *GetArgs, reply *GetReply, opChan chan Op) {
	for !kv.killed() {
		select {
		case appliedOp := <-opChan:
			DPrintf("Receive applied Op %+v", appliedOp)
			if appliedOp.Uuid != args.Uuid || appliedOp.ClientId != args.ClientId {
				DPrintf("skip applied Op %+v", appliedOp)
				continue
			}
			reply.Err = OK
			reply.Value = appliedOp.Value
			return
		default:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				DPrintf("kv %d not a leader anymore, return", kv.me)
				reply.Err = ErrWrongLeader
				return
			}
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	op := Op{Type: PutOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid, ClientId: args.ClientId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.opChan[index]; !ok {
		kv.opChan[index] = make(chan Op)
	}
	DPrintf("kv %d receive Put %+v at index %d ", kv.me, args, index)
	opChan := kv.opChan[index]

	if id, ok := kv.pendingOp[args.ClientId]; ok {
		if id != args.Uuid {
			delete(kv.opmap, id)
		}
	}
	kv.pendingOp[args.ClientId] = args.Uuid
	kv.mu.Unlock()

	kv.waitPutAppend(args, reply, opChan)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.opChan, index)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	op := Op{Type: AppendOp, Key: args.Key, Value: args.Value, Uuid: args.Uuid, ClientId: args.ClientId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.opChan[index]; !ok {
		kv.opChan[index] = make(chan Op)
	}
	DPrintf("kv %d receive Append %+v at index %d", kv.me, args, index)
	opChan := kv.opChan[index]

	if id, ok := kv.pendingOp[args.ClientId]; ok {
		if id != args.Uuid {
			delete(kv.opmap, id)
		}
	}
	kv.pendingOp[args.ClientId] = args.Uuid

	kv.mu.Unlock()

	kv.waitPutAppend(args, reply, opChan)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.opChan, index)
}

func (kv *KVServer) waitPutAppend(args *PutAppendArgs, reply *PutAppendReply, opChan chan Op) {
	for !kv.killed() {
		select {
		case appliedOp := <-opChan:
			DPrintf("Receive applied Op %+v", appliedOp)
			if appliedOp.Uuid != args.Uuid || appliedOp.ClientId != args.ClientId {
				DPrintf("skip applied Op %+v", appliedOp)
				continue
			}
			reply.Err = OK
			return
		}
	}
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
	kv.persister = persister

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.opmap = make(map[int64]Op)
	kv.kvmap = make(map[string]string)
	kv.opChan = make(map[int]chan Op)
	kv.pendingOp = make(map[int]int64)

	kv.Restore(kv.persister.ReadSnapshot())
	DPrintf("Restore from Snapshot %+v", kv)
	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			DPrintf("kv %d Receive Applied Op %+v", kv.me, op)
			kv.mu.Lock()
			kv.applyOp(&op)
			if _, isLeader := kv.rf.GetState(); isLeader {
				if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					DPrintf("Snapshot the raft state before %d", kv.persister.RaftStateSize())
					kv.Snapshot(msg.CommandIndex)
					DPrintf("Snapshot the raft state after %d", kv.persister.RaftStateSize())
				}
				DPrintf("kv %d Sent, %+v", kv.me, op)
				//DPrintf("op chan %+v", kv.opChan)
				if cha, ok := kv.opChan[msg.CommandIndex]; ok {
					cha <- op
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.Restore(msg.Snapshot)
			DPrintf("Restore from Snapshot %+v", msg)
		}
	}
}

func (kv *KVServer) applyOp(op *Op) {
	switch op.Type {
	case GetOp:
		if _, ok := kv.opmap[op.Uuid]; !ok {
			kv.opmap[op.Uuid] = *op
		}
		op.Value = kv.kvmap[op.Key]
	case PutOp:
		if _, ok := kv.opmap[op.Uuid]; !ok {
			kv.opmap[op.Uuid] = *op
			kv.kvmap[op.Key] = op.Value
		}
	case AppendOp:
		if _, ok := kv.opmap[op.Uuid]; !ok {
			kv.opmap[op.Uuid] = *op
			kv.kvmap[op.Key] += op.Value
		}
	}
}

func (kv *KVServer) Snapshot(lastIncludeIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//e.Encode(kv.opmap)
	e.Encode(kv.kvmap)
	e.Encode(kv.opmap)
	kv.rf.Snapshot(lastIncludeIndex, w.Bytes())
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[string]string
	var opmap map[int64]Op
	d.Decode(&kvmap)
	d.Decode(&opmap)
	kv.kvmap = kvmap
	kv.opmap = opmap
	kv.opChan = make(map[int]chan Op)
}
