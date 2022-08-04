package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct
	leaderId  int
	clientId  int64
	requestId int64
	delay     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = -1
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}
func (ck *Clerk) findLeader() {
	msgChan := make(chan string)
	doneId := -1
	for i, _ := range ck.servers {
		if ck.leaderId == -1 {
			go ck.GetRPC("", i, &msgChan, &doneId)
		}
	}
	select {
	case <-msgChan:
		if doneId != -1 {
			DPrintf(-1, "found leader %d ", doneId)
		}
		ck.leaderId = doneId
		ck.delay = 500
	case <-time.After(time.Duration(2000 * time.Millisecond)):
		DPrintf(1, "time out for find leader ")
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer
//
func (ck *Clerk) Get(key string) string {

	DPrintf(-1, "----clnt[%d] first try to get %v----", ck.clientId, key)
	msgChan := make(chan string)
	done := -1
	for {
		for ck.leaderId == -1 {
			ck.findLeader()
		}
		done = -1 //in case of some failure
		go ck.GetRPC(key, ck.leaderId, &msgChan, &done)
		select {
		case msg := <-msgChan:
			if done == -1 {
				ck.leaderId = -1
				DPrintf(-1, "----clnt[%d] claim not leader; retry to get %v----", ck.clientId, key)
			} else {
				return msg
			}
		case <-time.After(time.Second):
			DPrintf(-1, "clnt[%d] time out getting %v, leader:%d", ck.clientId, key, ck.leaderId)
			ck.leaderId = -1
			break
		}

	}

}
func (ck *Clerk) GetRPC(key string, i int, msg *chan string, doneId *int) {
	if *doneId >= 0 {
		return
	}
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	reply := GetReply{}

	ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
	// more check to reduce chan send
	if *doneId >= 0 {
		return
	}
	if !ok {
		DPrintf(2, "server [%d] failed for getting %v", i, key)
	} else if reply.Err == ErrWrongLeader {
		DPrintf(2, "server [%d] is not leader key %v", i, key)
	} else {
		*doneId = i
		DPrintf(2, "server [%d] success for getting %v", i, reply.Value)
	}
	// 如果没人接受消息， 超时后自己接受从而释放这个协程
	go func() {
		time.Sleep(time.Duration(ck.delay) * time.Millisecond)
		select {
		case <-*msg:
			ck.delay *= 2
			DPrintf(1, "clnt[%d] got get RPC but no accept chan time out", ck.clientId)
			break
		default:

		}
	}()
	// blocked
	*msg <- reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	done := false
	ck.requestId = nrand()
	msg := make(chan bool)
	DPrintf(-1, "----clnt[%d] first try to %v %v %v----", ck.clientId, op, key, value)
	for !done {

		for ck.leaderId == -1 {
			ck.findLeader()
		}
		DPrintf(-1, "----clnt[%d] retry to %v %v %v leader:%d done:%v----",
			ck.clientId, op, key, value, ck.leaderId, done)
		go ck.PutAppendRPC(key, value, op, ck.leaderId, &done, &msg)
		select {
		case <-msg:
			if done {
				DPrintf(0, "client[%d] success %v %v %v", ck.clientId, op, key, value)
				return
			} else {
				ck.leaderId = -1
				DPrintf(-1, "client[%d] failed %v %v %v", ck.clientId, op, key, value)
			}

		case <-time.After(2 * time.Second):
			DPrintf(-1, "time out PutAppend %v %v leader:%d", key, value, ck.leaderId)
			ck.leaderId = -1
		}
	}

}
func (ck *Clerk) PutAppendRPC(key string, value string, op string, i int, done *bool, msg *chan bool) {
	if *done {
		return
	}
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	reply := PutAppendReply{}
	//DPrintf(-1, "call for %v %v %v", op, key, value)

	ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
	if !ok || reply.Err != OK {
		DPrintf(-1, "failed for %v %v %v because %v", op, key, value, reply.Err)
	} else {
		*done = true
		DPrintf(-1, "success for %v %v %v", op, key, value)
	}
	// !!put 和 append 消息不应该被释放，否则会引起数据不一致!!?????
	// 如果没人接受消息， 超时后自己接受从而释放这个协程.
	go func() {
		time.Sleep(2 * time.Second)
		select {
		case <-*msg:
			break
		case <-time.After(time.Duration(100 * time.Millisecond)):
		}
	}()
	*msg <- true

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
