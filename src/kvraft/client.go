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
	requestId int
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
		go ck.GetRPC("", i, &msgChan, &doneId)
	}
	select {
	case <-msgChan:
		ck.leaderId = doneId
	case <-time.After(time.Duration(2000 * time.Millisecond)):
		DPrintf(1, "time out for find leader")
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

	// You will have to modify this function.
	DPrintf(1, "----first try to get %v----", key)
	msgChan := make(chan string)
	done := -1
	for {
		for ck.leaderId == -1 {
			ck.findLeader()
		}
		go ck.GetRPC(key, ck.leaderId, &msgChan, &done)
		select {
		case msg := <-msgChan:
			if done == -1 {
				ck.leaderId = -1
			} else {
				return msg
			}
		case <-time.After(time.Second):
			DPrintf(1, "time out getting %v", key)
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

	if !ok {
		DPrintf(2, "server [%d] failed for getting %v", i, key)
	} else if reply.Err == ErrWrongLeader {
		DPrintf(2, "server [%d] is not leader", i)
	} else {
		*doneId = i
		DPrintf(2, "server [%d] success for getting %v", i, reply.Value)
	}
	*msg <- reply.Value
	// 如果没人接受消息， 超时后自己接受从而释放这个协程
	go func() {
		time.Sleep(100 * time.Millisecond)
		select {
		case <-*msg:
			break
		case <-time.After(time.Duration(100 * time.Millisecond)):
		}
	}()
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
	ck.requestId++ // 一次任务只能自增一次
	msg := make(chan bool)
	DPrintf(1, "----first try to %v %v %v----", op, key, value)
	for {
		for ck.leaderId == -1 {
			ck.findLeader()
		}
		go ck.PutAppendRPC(key, value, op, ck.leaderId, &done, msg)
		select {
		case <-msg:
			if done {
				DPrintf(0, "client[%d] success %v %v %v", ck.clientId, op, key, value)
				return
			} else {
				ck.leaderId = -1
				DPrintf(0, "client[%d] failed %v %v %v", ck.clientId, op, key, value)
			}

		case <-time.After(2 * time.Second):
			DPrintf(1, "time out PutAppend %v %v", key, value)
			ck.leaderId = -1
		}
	}

}
func (ck *Clerk) PutAppendRPC(key string, value string, op string, i int, done *bool, msg chan bool) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	reply := PutAppendReply{}
	ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
	if *done {
		return
	}
	if !ok || reply.Err != OK {
		DPrintf(1, "failed for %v %v %v because %v", op, key, value, reply.Err)
	} else {
		*done = true
		DPrintf(1, "success for %v %v %v", op, key, value)
	}
	msg <- true

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
