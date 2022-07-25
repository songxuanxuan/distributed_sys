package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "github.com/sasha-s/go-deadlock"

const Debug = 0

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if Debug > level {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string
	ClientId  int64
	RequestId int
	Key       string
	Value     string
	OpChan    chan Result
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate  int               // snapshot if log grows this big
	logStorage    map[string]string //所有日志追加到后面
	lastRequestId map[int64]int     //每个客户端最后提交的索引, 新成为leader的要读取写在磁盘的这个保证不重复提交
	lastIndex     int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf(1, "[%d]trying getrpc %v", kv.me, args.Key)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	if len(args.Key) == 0 {
		reply.Err = OK
		reply.Value = ""
		return
	}
	//查找最后一个匹配的位置，即是最新的值
	opChan := make(chan Result)
	op := Op{
		Command:   "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     "",
		OpChan:    opChan,
	}
	msg := raft.ApplyMsg{
		CommandValid: true,
		Command:      op,
		CommandIndex: 0,
	}
	kv.applyCh <- msg
	info := fmt.Sprintf("[%d]... get %v", kv.me, args.Key)
	result := chanReceiver(opChan, info)
	reply.Err = result.Err
	reply.Value = result.Value
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf(1, "[%d] trying %v %v %v", kv.me, args.Op, args.Key, args.Value)
	opChan := make(chan Result)
	op := Op{
		Command:   args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
		OpChan:    opChan,
	}
	kv.mu.Lock()
	if kv.isDuplicate(&op) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	_, _, is := kv.rf.Start(op)
	if !is {
		reply.Err = ErrWrongLeader
		return
	}

	//一直等待日志提交
	//result := <-opChan
	info := fmt.Sprintf("[%d]waiting receive... %v %v", kv.me, args.Op, args.Value)
	result := chanReceiver(opChan, info)
	reply.Err = result.Err
	if result.Ok {
		DPrintf(0, "get applied %v", kv.logStorage[args.Key])
	} else {
		DPrintf(0, "get applied failed because %v", result.Err)

	}
}

type Result struct {
	Ok    bool
	Value string
	Err   Err
}

func (kv *KVServer) isDuplicate(op *Op) bool {
	requestId, ok := kv.lastRequestId[op.ClientId]
	if ok {
		return requestId >= op.RequestId
	}
	return false
}

//
func chanSender(chanWay chan Result, msg Result, info string) {
	DPrintf(1, "begin chan %v, value %v", info, msg)
	go func() {
		// 检测2秒后，客户端的rpc还没把结果取走就认为网络掉了，自己关闭这个chan
		time.Sleep(2 * time.Second)
		select {
		case <-chanWay:
			DPrintf(1, "client receive chan time out chan %v, value %v", info, msg)
			return
		case <-time.After(2 * time.Second):
			return
		}
	}()
	chanWay <- msg
}
func chanReceiver(chanWay chan Result, info string) Result {
	DPrintf(1, "begin chan %v ", info)
	select {
	case msg := <-chanWay:
		DPrintf(1, "end chan %v value %v", info, msg)
		return msg
	case <-time.After(time.Second * 3):
		DPrintf(1, "end chan %v because time out ", info)
		return Result{Ok: false, Value: "", Err: ErrWrongLeader}
	}

}
func (kv *KVServer) opResolver(op *Op) Result {
	result := Result{
		Ok:    false,
		Value: "",
		Err:   OK,
	}
	kv.mu.Lock()
	switch op.Command {
	case "Get":
		if value, ok := kv.logStorage[op.Key]; ok {
			result.Value = value
			result.Ok = true
		} else {
			result.Err = ErrNoKey
		}
		break
	case "Put":
		if kv.isDuplicate(op) {
			kv.mu.Unlock()
			return result
		}
		kv.logStorage[op.Key] = op.Value
		kv.lastRequestId[op.ClientId] = op.RequestId

		result.Ok = true
		break
	case "Append":
		if kv.isDuplicate(op) {
			kv.mu.Unlock()
			return result
		}
		kv.logStorage[op.Key] += op.Value
		kv.lastRequestId[op.ClientId] = op.RequestId
		result.Ok = true
		break
	}
	kv.mu.Unlock()
	return result

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	deadlock.Opts.Disable = false
	deadlock.Opts.DeadlockTimeout = time.Second * 3
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.lastRequestId = make(map[int64]int)
	kv.logStorage = make(map[string]string)
	kv.lastIndex = -1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//单独开线程来接受apply
	go kv.receiveApply()

	return kv
}
func (kv *KVServer) receiveApply() {
	for {
		if kv.dead == 1 {
			return
		}
		//DPrintf(1, "waiting for applych......")
		msg := <-kv.applyCh
		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)

		if op.Command != "Get" && msg.CommandIndex <= kv.lastIndex {
			// 避免多次apply产生过多的chan消息.
			continue
		}
		kv.lastIndex = msg.CommandIndex
		result := kv.opResolver(&op)
		if !result.Ok {
			continue
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		DPrintf(1, "[%d] got raft applied %v %v %v index:%v-%v", kv.me, op.Command, op.Key, op.Value, kv.lastIndex, msg.CommandIndex)
		info := fmt.Sprintf("[%d]to send msg from %v %v", kv.me, op.Command, op.Value)
		go chanSender(op.OpChan, result, info) //将结果发送给相应的请求客户端
		kv.mu.Lock()
		DPrintf(1, "[%d] logStorage %v", kv.me, kv.logStorage)
		kv.mu.Unlock()
	}
}
