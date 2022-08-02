package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"math/rand"
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
	RequestId int64
	Key       string
	Value     string
	ChanKey   int
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate     int                      // snapshot if log grows this big
	logStorage       map[string]string        //所有日志追加到后面
	lastRequestId    map[int64]map[int64]bool //每个客户端最后提交的索引, 新成为leader的要读取写在磁盘的这个保证不重复提交
	clientChan       map[int]chan Result      //每个请求端维护一个通道
	muChan           deadlock.Mutex
	lastIndex        int
	lastTerm         int
	lastIncludeIndex int
	lastIncludeTerm  int
}

func (kv *KVServer) createClientChan() int {
	kv.muChan.Lock()
	defer kv.muChan.Unlock()
	rand.Seed(time.Now().UnixNano())
	ok := true
	key := 0
	for ok {
		key = rand.Int()
		_, ok = kv.clientChan[key]
	}
	kv.clientChan[key] = make(chan Result)
	return key
}
func (kv *KVServer) freeClientChan() {

}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//if kv.killed() {
	//	reply.Err = ErrWrongLeader
	//	return
	//}
	DPrintf(1, "[%d]trying get rpc %v", kv.me, args.Key)
	_, isLeader := kv.rf.GetState()
	// todo: available
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	// for finding leader
	if len(args.Key) == 0 {
		reply.Err = OK
		reply.Value = ""
		return
	}
	if !kv.rf.IsAvailable() {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	DPrintf(1, "[%d]trying get rpc %v leader", kv.me, args.Key)
	//chanKey := kv.createClientChan()
	//查找最后一个匹配的位置，即是最新的值
	//op := Op{
	//	Command:   "Get",
	//	ClientId:  args.ClientId,
	//	RequestId: args.RequestId,
	//	Key:       args.Key,
	//	Value:     "",
	//	ChanKey:   chanKey,
	//}
	//msg := raft.ApplyMsg{
	//	CommandValid: true,
	//	Command:      op,
	//	CommandIndex: 0,
	//}
	//_, _, is := kv.rf.Start(op)
	//if !is {
	//	DPrintf(1, "-------[%d] wrong leader after start", kv.me)
	//	reply.Err = ErrWrongLeader
	//	return
	//}

	//info := fmt.Sprintf("[%d]... get %v", kv.me, args.Key)
	//result := kv.chanReceiver(chanKey, info)
	//reply.Err = result.Err
	//reply.Value = result.Value
	kv.mu.Lock()
	if value, ok := kv.logStorage[args.Key]; ok {
		reply.Value = value
	} else {
		//DPrintf(-1, "[%d] err no key : %v, log: %v", kv.me, op.Key, kv.logStorage)
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//if kv.killed() {
	//	reply.Err = ErrWrongLeader
	//	return
	//}
	DPrintf(1, "[%d] trying %v %v %v", kv.me, args.Op, args.Key, args.Value)
	op := Op{
		Command:   args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}
	kv.mu.Lock()
	if kv.isDuplicate(&op) {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	chanKey := kv.createClientChan()
	op.ChanKey = chanKey
	_, _, is := kv.rf.Start(op)
	if !is {
		reply.Err = ErrWrongLeader
		return
	}

	//一直等待日志提交
	//result := <-opChan
	info := fmt.Sprintf("[%d]waiting receive... %v %v", kv.me, args.Op, args.Value)
	result := kv.chanReceiver(chanKey, info)
	reply.Err = result.Err
	if result.Ok {
		//DPrintf(0, "get applied %v", kv.logStorage[args.Key])
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
	_, ok := kv.lastRequestId[op.ClientId]
	if ok {
		_, exist := kv.lastRequestId[op.ClientId][op.RequestId]
		DPrintf(3, "[%d] isDuplicate:%v new requestId when %v %v,id:%v, ids:%v", kv.me, exist, op.Command, op.Value, op.RequestId, kv.lastRequestId[op.ClientId])
		return exist
	}
	return false
}

//
func (kv *KVServer) chanSender(chanKey int, msg Result, info string) {
	kv.muChan.Lock()
	clientChan := kv.clientChan[chanKey]
	kv.muChan.Unlock()
	DPrintf(1, "begin chan %v, value %v", info, msg)
	go func() {
		// 检测2秒后，客户端的rpc还没把结果取走就认为网络掉了，自己关闭这个chan
		time.Sleep(2 * time.Second)
		select {
		case <-clientChan:
			DPrintf(1, "client receive chan time out chan %v, value %v", info, msg)
			//kv.clientChan[chanKey] = nil
			return
		case <-time.After(2 * time.Second):
			return
		}
	}()
	clientChan <- msg
}
func (kv *KVServer) chanReceiver(chanKey int, info string) Result {
	kv.muChan.Lock()
	clientChan := kv.clientChan[chanKey]
	kv.muChan.Unlock()
	DPrintf(1, "begin chan %v ", info)
	select {
	case msg := <-clientChan:
		DPrintf(1, "end chan %v value %v", info, msg)
		//kv.clientChan[chanKey] = nil

		return msg
	case <-time.After(time.Second * 1):
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
		} else {
			//DPrintf(-1, "[%d] err no key : %v, log: %v", kv.me, op.Key, kv.logStorage)
			result.Err = ErrNoKey
		}
		result.Ok = true
		break
	case "Put":
		if kv.isDuplicate(op) {
			kv.mu.Unlock()
			return result
		}
		kv.logStorage[op.Key] = op.Value
		kv.fillRequestId(op.ClientId, op.RequestId)

		//kv.lastRequestId[op.ClientId][op.RequestId] = true

		result.Ok = true
		break
	case "Append":
		if kv.isDuplicate(op) {
			kv.mu.Unlock()
			return result
		}
		kv.logStorage[op.Key] += op.Value
		//todo:判断重复命令
		kv.fillRequestId(op.ClientId, op.RequestId)
		//kv.lastRequestId[op.ClientId][op.RequestId] = true
		result.Ok = true
		break
	}
	kv.mu.Unlock()
	return result

}
func (kv *KVServer) fillRequestId(clientId int64, requestId int64) {
	_, ok := kv.lastRequestId[clientId]
	if !ok {
		kv.lastRequestId[clientId] = make(map[int64]bool)
	}
	kv.lastRequestId[clientId][requestId] = true
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
	DPrintf(3, "[%d] killing log: %v", kv.me, kv.logStorage)
	//kv.logStorage = nil
	//kv.lastRequestId = nil
	//kv.freeClientChan()
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
	deadlock.Opts.Disable = true
	deadlock.Opts.DeadlockTimeout = time.Second * 3
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.lastRequestId = make(map[int64]map[int64]bool)
	kv.logStorage = make(map[string]string)
	kv.clientChan = make(map[int]chan Result)
	kv.lastIndex = -1
	kv.lastTerm = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.recoverSnapshot()
	kv.recoverData()
	//单独开线程来接受apply
	go kv.receiveApply()

	return kv
}

// 根据raft的log恢复kv数据
func (kv *KVServer) recoverData() {
	logP := kv.rf.ExposeLog()

	if len(*logP) > 1 {
		//DPrintf(-1, "[%d] recover log: %v", kv.me, logP)
		for _, l := range (*logP)[1:] {
			op := l.Command.(Op)
			if op.Command == "Get" {
				continue
			} else if op.Command == "Put" {
				if !kv.isDuplicate(&op) {
					kv.logStorage[op.Key] = op.Value
				}
			} else {
				if !kv.isDuplicate(&op) {
					kv.logStorage[op.Key] += op.Value
				}
			}
			kv.fillRequestId(op.ClientId, op.RequestId)

			//kv.lastRequestId[op.ClientId][op.RequestId] = true
		}
		DPrintf(3, "[%d] restart LOG :%v", kv.me, kv.logStorage)
	}
}

func (kv *KVServer) saveSnapshot() {
	if kv.maxraftstate < 0 {
		return
	}
	if kv.rf.ShouldSnapshot(kv.maxraftstate) {
		DPrintf(1, "[%d] saveSnapshot ", kv.me)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		kv.mu.Lock()
		kv.lastIncludeIndex = kv.lastIndex
		kv.lastIncludeTerm = kv.lastTerm
		e.Encode(kv.logStorage)
		e.Encode(kv.lastIncludeIndex)
		e.Encode(kv.lastIncludeTerm)
		kv.mu.Unlock()
		data := w.Bytes()
		kv.rf.SaveStateAndSnapshot(kv.lastIndex, data)
	}
}

func (kv *KVServer) recoverSnapshot() {
	data := kv.rf.ReadSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logStorage map[string]string
	var lastIncludeIndex int
	var lastTerm int
	if d.Decode(&logStorage) != nil || d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastTerm) != nil {
		DPrintf(-99, "[%d] kv recoverSnapshot failed", kv.me)
	} else {
		kv.logStorage = logStorage
		kv.lastIncludeIndex = lastIncludeIndex
		kv.lastTerm = lastTerm
	}
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
		kv.lastTerm = msg.CommandTerm
		result := kv.opResolver(&op)
		kv.mu.Lock()
		DPrintf(3, "[%d] after apply logStorage %v", kv.me, kv.logStorage)
		kv.mu.Unlock()
		//DPrintf(-1, "[%d] got applych...... value %v %v result %v", kv.me, op.Command, op.Key, result.Err)

		if !result.Ok {
			continue
		}
		go kv.saveSnapshot()

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		DPrintf(1, "[%d] got raft applied %v %v %v index:%v-%v", kv.me, op.Command, op.Key, op.Value, kv.lastIndex, msg.CommandIndex)
		info := fmt.Sprintf("[%d]to send msg from %v %v", kv.me, op.Command, op.Value)
		go kv.chanSender(op.ChanKey, result, info) //将结果发送给相应的请求客户端

	}
}
