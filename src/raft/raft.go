package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"bytes"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "github.com/sasha-s/go-deadlock"

//import "../labgob"

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const ElectionInterval = 300
const HeartBeatInterval = 150

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu             deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	state          int                 //身份
	randomInterval int32               //随机生成的选举超时时间
	electionFlag   chan bool           // 重置选举后通知标志
	applyChan      chan ApplyMsg       // 发起日志应用通过该通道
	muApply        deadlock.Mutex      // 专门用于提交apply的锁
	available      bool                //处于少数分区的leader，拒绝start读写操作
	nOK            int                 //正常接受的心跳的节点数量
	muOK           deadlock.Mutex
	// 非易失性数据
	lastSnapshotIndex int
	lastSnapshotTerm  int
	currentTerm       int   // 当前服务器已知的最新任期(初始化为0)
	votedFor          int   // 当前任期内投给选票的候选者id, 如果没投为空
	log               []Log //日志条目; 每个log包含状态机的命令, leader收到命令时的任期(第一个索引为1!!)
	// 易失性数据(yet stored)
	// note: 在lastApplied和commitIndex之间是已经认可, 但是还没有提交的log
	lastApplied int //最后一个已经提交了的log index(初始0)
	commitIndex int //最后一个已经超过半数认可的index(初始0)

	// leader 专用, 易失性数据
	nextIndex []int // 每个follower的下一条需要同步的log(初始化为leader最后的log index+1
	matchIdex []int //每个follower最大的已同步的log, 尚未提交(初始为0)

}
type LogIndex struct {
	idx    int
	nodeId int
}
type Log struct {
	Command interface{}
	Term    int
	Idx     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf(-1, "[%d] getState : %v", rf.me, rf.state)
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.generateState())

}
func (rf *Raft) generateState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	rf.mu.Unlock()
	data := w.Bytes()
	//DPrintf(-1, "[%d] generateState len %d", rf.me, len(data))
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []Log
	var votedFor int
	var lastSnapShotIndex int
	var lastSnapShotTerm int
	var currentTerm int
	var commitIndex int
	var lastApplied int
	if d.Decode(&log) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&lastSnapShotIndex) != nil || d.Decode(&lastSnapShotTerm) != nil ||
		d.Decode(&currentTerm) != nil || d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf(1, "[%d] decode failed", rf.me)
	} else {
		rf.log = log
		rf.votedFor = votedFor
		rf.lastSnapshotIndex = lastSnapShotIndex
		rf.lastSnapshotTerm = lastSnapShotTerm
		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
	}
	DPrintf(1, "[%d] reading Persist, voteF:%d, term:%d, log:%v",
		rf.me, rf.votedFor, rf.currentTerm, rf.log)
}

func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) ExposeLog() *[]Log {
	return &rf.log
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Offset            int // not use
	Done              int //not use
}

type InstallSnapshotReply struct {
	Term int
}

// RequestVote
// 候选者发起的投票请求, 由候选者调用follower的该请求
//	如果term小于follower的term返回false;
//	如果follower的voteFor为空或等于候选人的id, 并且候选人的日志比follower的新(logTerm大于等于, 或logId大于等于)
//	return 任期号, 是否投票.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	DPrintf(0, "[%d] got request vote from %d, term:%d-%d, lastCommit %d",
		rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.commitIndex)
	rf.resetRandomInterval()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	lastEntry := rf.getLastEntry()
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		// 如果term比较大, 日志则至少一样新
		if (args.LastLogTerm > lastEntry.Term) ||
			((args.LastLogTerm == lastEntry.Term) && args.LastLogIndex >= lastEntry.Idx) {

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			// 如果发现该节点日志比较新, 返回intMax使其返回follower状态
			reply.Term = math.MaxInt
		}

	} else {
		// 如果term一样, 投票需要保证, votefor一样或为空, 并且日志至少一样新
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			((args.LastLogTerm > lastEntry.Term) ||
				((args.LastLogTerm == lastEntry.Term) && args.LastLogIndex >= lastEntry.Idx)) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			// 如果发现该节点日志比较新, 返回intMax使其返回follower状态
			reply.Term = math.MaxInt
		}
	}

	rf.mu.Unlock()
	rf.persist() // 持久化操作.

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	term = rf.currentTerm
	if rf.state != LEADER { // 如果不是leader或者处于少数分区
		isLeader = false
	} else if rf.available == false { //todo:可能available更新不及时
		//if !rf.preVoteAux() {
		//	DPrintf(1, "[%d] available is false", rf.me)
		//	isLeader = false
		//} else {
		//	rf.available = true
		//}
		DPrintf(-1, "[%d] available is false", rf.me)
		isLeader = false
	} else {
		index = rf.Commit(command)
	}

	//index = len(rf.log) - 1
	return index, term, isLeader
}
func (rf *Raft) IsAvailable() bool {
	return rf.state == LEADER && rf.available
}

func (rf *Raft) Commit(command interface{}) int {
	rf.mu.Lock()
	index := len(rf.log) + rf.lastSnapshotIndex
	DPrintf(0, "[%d] beginning a commit in idx %d,term %d", rf.me, index, rf.currentTerm)

	newLog := Log{
		Command: command,
		Term:    rf.currentTerm,
		Idx:     index,
	}
	rf.log = append(rf.log, newLog)
	// 设置自己的状态
	rf.matchIdex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.mu.Unlock()
	rf.persist() // 持久化.
	return index
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //leader log[] 中新日志前一个索引
	PrevLogTerm  int //leader log[] 中新日志前一个任期
	Entries      []Log
	LeaderCommit int //领导已知已提交的最高日志索引
}

type AppendEntriesReply struct {
	Term    int  //当前领导者任期
	Success bool // 如果PrevLogIndex PrevLogTerm 匹配上了为true
	//LastIndex int  //last matched index
}

// AppendEntries leader复制log; 心跳
// 由leader发起, entries为空为心跳, 否则为日志操作
//	如果接受方收到的任期<currentTerm, 返回false
//	如果接受方收到的任期>currentTerm, 令currentTerm=任期, 并切换为follower
//	如果在接收方(follower)日志中没有匹配的PrevLogIndex,返回false (先比较索引, 再任期)
//	如果日志中任期匹配的 PrevLogTerm却不相等, 删除这个不相等的及其以后的, 追加新的日志; todo 减少nextIndex的值重试, 知道找到匹配的哪一个, 其后的日志再全部同步过来
//	如果LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, 发送来的entries[]里面最大的索引值)
//	更新commitIndex, 值为matchIndex中的索引中位数

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	DPrintf(1, "[%d] got append/beat from %d, term %d-%d, snapshot:%d,len of entries %d",
		rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.lastSnapshotIndex, len(args.Entries))
	if rf.currentTerm > args.Term {
		// leader term 比自己小
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	// 成功返回心跳
	rf.currentTerm = args.Term
	reply.Term = args.Term
	rf.state = FOLLOWER
	reply.Success = true
	rf.votedFor = -1
	rf.mu.Unlock()
	// 重置选举计时器
	rf.resetRandomInterval()

	// 更新commitIndex.
	//	如果是心跳, 说明之前已经完成复制了, apply即可	*
	//	如果携带log, 追加log再进行apply	x
	if len(args.Entries) == 0 {
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			go rf.apply()
		}
		// 如果是心跳, 就结束.
		return
	}
	DPrintf(1, "[%d] got append from %d, term %d-%d, snapshot:%d,len of entries %d",
		rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.lastSnapshotIndex, len(args.Entries))
	lastId := args.Entries[len(args.Entries)-1].Idx
	//todo: 非领导者的log[] 最后一个log是commitIndex,
	DPrintf(1, "[%d] received prevIndex %d, prevTerm %d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
	rf.mu.Lock()

	// 如果prev index 大于lastSnapshotIndex 正常同步， 小于说明需要进行snapshotRPC
	if (args.PrevLogIndex == rf.lastSnapshotIndex && args.PrevLogTerm == rf.lastSnapshotTerm) ||
		(args.PrevLogIndex > rf.lastSnapshotIndex && rf.getEntryByIndex(args.PrevLogIndex).Term == args.PrevLogTerm) {
		// 成功找到符合的log,直接追加.
		rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSnapshotIndex], args.Entries...)
		DPrintf(1, "[%d] update data logs: %v", rf.me, rf.log)
		// 更新commitIndex
		//	如果是心跳, 说明之前已经完成复制了, apply即可 x
		//	如果携带log, 追加log再进行apply *
		if args.LeaderCommit > rf.commitIndex {

			if lastId > args.LeaderCommit {
				lastId = args.LeaderCommit
			}
			rf.commitIndex = lastId
			go rf.apply()
		}

	} else {
		// prev处的term不同 删除后面的log, 后续重新请求.直接覆盖即可, leader的log长度必不小于普通节点.
		//rf.log = append(rf.log[:args.PrevLogIndex+1], rf.log[rf.commitIndex+1:]...)
		DPrintf(1, "[%d] update failed data logs: %v", rf.me, rf.log)
		//reply.LastIndex = rf.commitIndex
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()
	DPrintf(2, "[%d] received prevIndex %d, prevTerm %d unlock-----", rf.me, args.PrevLogIndex, args.PrevLogTerm)
	rf.persist() // 持久化

	//for i := rf.commitIndex; i >= 0; i-- {
	//	if rf.log[i].Idx == args.PrevLogIndex {
	//		if rf.log[i].Term == args.PrevLogTerm {
	//			// 索引相等, term相等, 可证明之前的日志都相等
	//			rf.log = append(rf.log[:i+1], args.Entries...)
	//		} else {
	//			//todo: 删除后面的log, 后续重新请求追加新的
	//			rf.log = append(rf.log[:i], rf.log[rf.commitIndex+1:]...)
	//			reply.Success = false
	//			return
	//		}
	//	}
	//}

}
func (rf *Raft) getNextRelativeIndex(server int) int {
	if rf.nextIndex[server] > rf.lastSnapshotIndex {
		return rf.nextIndex[server] - rf.lastSnapshotIndex
	} else {
		return 1
	}
}
func (rf *Raft) getPrevTerm(server int) int {
	if rf.nextIndex[server]-1 > rf.lastSnapshotIndex {
		return rf.log[rf.nextIndex[server]-1-rf.lastSnapshotIndex].Term
	} else {
		return rf.lastSnapshotTerm
	}
}
func (rf *Raft) getPrevIndex(server int) int {
	if rf.nextIndex[server]-1 > rf.lastSnapshotIndex {
		return rf.nextIndex[server] - 1
	} else {
		return rf.lastSnapshotIndex
	}
}

// 发送心跳/同步
func (rf *Raft) SendAppendEntries(server int) bool {
	//DPrintf(0, "[%d] sending heartbeat/append to %d\n", rf.me, server)

	rf.mu.Lock()
	endOfLog := rf.log[len(rf.log)-1].Idx
	// todo: prev change to match index

	//defer func() {
	//	if r := recover(); r != nil {
	//		DPrintf(-999, "[%d] out of range logs %v relative %v", rf.me, rf.log, entries)
	//		log.Fatal(r)
	//		os.Exit(1)
	//	}
	//}()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getPrevIndex(server), //absolute index
		PrevLogTerm:  rf.getPrevTerm(server),
		Entries:      rf.log[rf.getNextRelativeIndex(server):], // 如果都完成了同步, 正好为空作为心跳
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	ok := rf.sendAppendEntriesAux(server, &args, &reply)
	if !ok || args.Term != rf.currentTerm || rf.state != LEADER {
		return ok
	}
	// 发现其他人返回的term比较大, 或者日志比较新, 退出领导人
	// reply.Term > rf.currentTerm
	if !reply.Success {
		if reply.Term > rf.currentTerm {
			// 发现别的term更大
			rf.ConverToFollower(reply.Term)
		} else {
			// 没有找到吻合的entry, 重试同步
			DPrintf(2, "[%d] commit failed occur when log=%v", rf.me, rf.log)
			if rf.nextIndex[server] > rf.lastSnapshotIndex {
				rf.nextIndex[server]--
				//rf.matchIdex[server]--
			} else {
				DPrintf(-1, "[%d] sendInstallSnapshot to %d", rf.me, server)
				go rf.sendInstallSnapshot(server)
			}

		}

	} else if rf.nextIndex[server] <= endOfLog {
		//成功同步.
		rf.matchIdex[server] = endOfLog
		rf.nextIndex[server] = endOfLog + 1
		DPrintf(1, "[%d] modify next index[%d] :%d", rf.me, server, rf.nextIndex[server])
		rf.resetCommitIndex()
		go rf.apply()
	} else {
		DPrintf(1, "[%d] success end:%d = nextindex[%d] %d", rf.me, endOfLog, server, rf.nextIndex[server])

	}
	return ok
}

// leader 根据matchIndex[]设置commitIndex..
func (rf *Raft) resetCommitIndex() {
	rf.mu.Lock()
	idx_cp := make([]int, len(rf.matchIdex))
	copy(idx_cp, rf.matchIdex)
	sort.Ints(idx_cp)
	if rf.commitIndex < idx_cp[len(idx_cp)/2] {
		rf.commitIndex = idx_cp[len(idx_cp)/2]
	}
	if rf.commitIndex < rf.lastApplied {
		rf.commitIndex = rf.lastApplied
	}

	DPrintf(1, "[%d] resetCommitIndex to %d", rf.me, rf.commitIndex)

	rf.mu.Unlock()
	rf.persist() // 持久化操作

}

// 应用已经同步的log
func (rf *Raft) apply() {
	rf.mu.Lock()
	if rf.state == LEADER {
		DPrintf(1, "[%d] begin apply lastApplied:%d, commitIndex:%d", rf.me, rf.lastApplied, rf.commitIndex)
	}
	applied := 0
	if rf.lastApplied > rf.lastSnapshotIndex {
		applied = rf.lastApplied - rf.lastSnapshotIndex
	}
	commitTo := rf.commitIndex - rf.lastSnapshotIndex
	rf.lastApplied = rf.commitIndex //先设置完成，避免其他线程同时进行apply
	//rf.mu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			DPrintf(-999, "[%d] out of range logs %v applied:%d,commit:%d,shotIndex:%d",
				rf.me, rf.log, applied, commitTo, rf.lastSnapshotIndex)
			log.Fatal(r)
			os.Exit(1)
		}
	}()
	var apps []ApplyMsg
	for ; applied < commitTo; applied++ {
		DPrintf(1, "[%d] apply send log :%v", rf.me, rf.log[applied+1].Command)
		//rf.mu.Lock()
		app := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[applied+1].Command,
			CommandIndex: applied + 1 + rf.lastSnapshotIndex, //save as abs index
			CommandTerm:  rf.log[applied+1].Term,
		}
		//rf.mu.Unlock()
		apps = append(apps, app)
		//if rf.state == LEADER {
		//	DPrintf(-1, "[%d] apply to chan command:%v appliedId %d,commitTo %d", rf.me, app.Command, applied, commitTo)
		//}

	}
	rf.mu.Unlock()
	rf.muApply.Lock()
	for _, app := range apps {
		rf.applyChan <- app
	}
	rf.muApply.Unlock()
	rf.persist() // 持久化操作

	DPrintf(1, "[%d] apply completed lastApplied:%d", rf.me, rf.lastApplied)

}

func (rf *Raft) sendAppendEntriesAux(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// leader 初始化两个数组
func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIdex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
}

// OperateLeader
//	leader 给其他发送心跳
func (rf *Raft) OperateLeader(server int, ok *int, mutex *deadlock.Mutex) {
	//DPrintf(2, "[%d] begin sending heartbeat to %d\n", rf.me, server)
	if rf.nextIndex[server] > rf.lastSnapshotIndex {
		if rf.SendAppendEntries(server) {
			mutex.Lock()
			*ok++
			mutex.Unlock()
		}
	} else {
		rf.sendInstallSnapshot(server) //todo: forever loop if not success for now
	}

}
func (rf *Raft) leaderManager() {
	ok := 1
	mu := deadlock.Mutex{}
	for {
		if rf.dead == 1 {
			return
		}
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok = 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.OperateLeader(i, &ok, &mu)
			}
		}

		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
		//等待检查是否超过半数节点成功接受心跳
		if ok < (len(rf.peers)+1)/2 && !rf.preVoteAux() {
			rf.available = false
			DPrintf(1, "[%d] set available false", rf.me)
		} else {
			rf.available = true
			DPrintf(1, "[%d] set available true okn:%d", rf.me, ok)
		}
	}

}

func (rf *Raft) getLastEntry() Log {
	if rf.commitIndex == 0 {
		return Log{}
	}
	if rf.commitIndex >= len(rf.log) {
		return rf.log[len(rf.log)-1]
	}
	return rf.log[rf.commitIndex]
}
func (rf *Raft) getLastApplied() Log {
	if rf.lastApplied == 0 {
		return Log{}
	}
	return rf.log[rf.lastApplied]
}

// 设置随机时间
func (rf *Raft) resetRandomInterval() {
	// 设置随机时间在150-300毫秒

	rand.Seed(time.Now().UnixNano())
	//rf.mu.Lock()
	atomic.StoreInt32(&rf.randomInterval, rand.Int31n(ElectionInterval)+ElectionInterval)
	//rf.randomInterval = rand.Intn(ElectionInterval) + ElectionInterval
	//rf.mu.Unlock()
	go func() {
		rf.electionFlag <- true
	}()

}

// Tick  超时后进行参与选举
func (rf *Raft) Tick() {
	DPrintf(0, "[%d] started!", rf.me)

	for {
		if rf.dead == 1 {
			return
		}
		interval := atomic.LoadInt32(&rf.randomInterval)
		//rf.mu.Lock()
		//interval := rf.randomInterval
		//rf.mu.Unlock()
		select {
		case <-rf.electionFlag: // 重置计时器.
			DPrintf(1, "[%d] reset tick...time=%d", rf.me, rf.randomInterval)
			break
		case <-time.After(time.Duration(interval) * time.Millisecond):
			if rf.state != LEADER {
				DPrintf(1, "[%d] timeout tick... begin election", rf.me)
				rf.AttemptElection()
			}
		}

	}

}

type PreVoteArgs struct {
}

type PreVoteReply struct {
	Connected bool
}

func (rf *Raft) PreVote(args *PreVoteArgs, reply *PreVoteReply) {
	reply.Connected = true
}

// 查看连接状态,处于大多数分区返回true
func (rf *Raft) preVoteAux() bool {
	DPrintf(1, "[%d] preVoting/check connecting...", rf.me)
	got := make(chan bool)
	count := 1
	mu := deadlock.Mutex{}
	for i := 0; i < len(rf.peers); i++ {
		if count >= (len(rf.peers)+1)/2 {
			break
		}
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := PreVoteReply{}
			args := PreVoteArgs{}
			ok := rf.peers[i].Call("Raft.PreVote", &args, &reply)
			if ok && reply.Connected {
				mu.Lock()
				count++
				mu.Unlock()
				if count >= (len(rf.peers)+1)/2 {
					got <- true
				}
			}
		}(i)
	}

	select {
	case <-got:
		return true
	case <-time.After(50 * time.Millisecond): //todo: 时间有影响吗
		DPrintf(1, "[%d] preVote/check connecting. failed", rf.me)
		return false
	}

}

// AttemptElection 开始竞选, 发送给所有其他服务器.
func (rf *Raft) AttemptElection() {
	if !rf.preVoteAux() {
		return
	}

	rf.mu.Lock()
	DPrintf(2, "[%d] success preVote in Term %d\n", rf.me, rf.currentTerm)
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf(0, "[%d] attempting an election in Term %d\n", rf.me, rf.currentTerm)
	term := rf.currentTerm

	rf.mu.Unlock()
	rf.persist() // 持久化

	total := len(rf.peers)
	voteCount := 1
	var countMu deadlock.Mutex
	finished := 0
	done := false
	accepted := make(chan bool)
	for server, _ := range rf.peers {
		finished++
		if server == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.CallRequestVote(server, term)
			if !voteGranted {
				return
			}

			countMu.Lock()
			voteCount++
			countMu.Unlock()
			// 超过半数投票即可
			if done || voteCount < (total+1)/2 {
				return
			}
			done = true
			if term == rf.currentTerm && rf.state == CANDIDATE {
				DPrintf(2, "[%d] got voteCount %d", rf.me, voteCount)
				accepted <- true
			}
		}(server)
	}
	for {
		select {
		case <-accepted:
			rf.ConverToLeader()
			// 开始发送心跳
			go rf.leaderManager()

			return
		case <-time.After(time.Millisecond * time.Duration(rf.randomInterval)): //超时就不再等待
			// 重新尝试
			//DPrintf(0, "[%d] retry election (nextTerm= %d)\n", rf.me, term)
			//if rf.state == CANDIDATE {
			//	rf.AttemptElection()
			//}
			return
		}
	}

}

func (rf *Raft) CallRequestVote(server int, term int) bool {
	rf.mu.Lock()
	DPrintf(0, "[%d] sending request vote to %d, lastCommit %d", rf.me, server, rf.commitIndex)
	lastEntry := rf.getLastEntry()
	rf.mu.Unlock()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastEntry.Idx,
		LastLogTerm:  lastEntry.Term,
	}
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	DPrintf(0, "[%d] finish sending request vote to %d, got? %v", rf.me, server, reply.VoteGranted)
	if reply.Term > rf.currentTerm {

		rf.state = FOLLOWER
		rf.votedFor = -1

		rf.mu.Unlock()
		rf.persist() // 持久化.

		//rf.currentTerm = reply.Term
	} else {
		rf.mu.Unlock()
	}

	return reply.VoteGranted
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	//rf.log = nil
	DPrintf(0, "[%d] to be shutdown!", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getEntryByIndex(index int) Log {
	if index-rf.lastSnapshotIndex >= len(rf.log) {
		return Log{Term: -1}
	}
	return rf.log[index-rf.lastSnapshotIndex]
}

func (rf *Raft) SaveStateAndSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()

	if index > rf.lastApplied {
		//rf.mu.Unlock()
		panic("SaveStateAndSnapshot error,index > rf.lastApplied ")
	}
	if index <= rf.lastSnapshotIndex {
		rf.mu.Unlock()
		return
	}
	rf.lastSnapshotTerm = rf.getEntryByIndex(index).Term
	rf.log = append(rf.log[:1], rf.log[index-rf.lastSnapshotIndex+1:]...)

	rf.lastSnapshotIndex = index
	DPrintf(-1, "[%d] snapshoted, LastIncludedIndex %d, LastIncludedTerm %d",
		rf.me, rf.lastSnapshotIndex, rf.lastSnapshotTerm)
	rf.mu.Unlock()
	state := rf.generateState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

}
func (rf *Raft) InstallSnapshotRpc(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm || rf.state != FOLLOWER {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.mu.Unlock()
		rf.resetRandomInterval()
		return
	}
	if args.LastIncludedIndex < rf.lastSnapshotIndex {
		rf.mu.Unlock()
		return
	}
	// trim logs before index
	realIndex := args.LastIncludedIndex - rf.lastSnapshotIndex
	if realIndex >= len(rf.log) {
		rf.log = rf.log[:1]
	} else {
		rf.log = append(rf.log[:1], rf.log[realIndex+1:]...)
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	DPrintf(-1, "[%d] RPC snapshoted,LastIncludedIndex %d, LastIncludedTerm %d",
		rf.me, rf.lastSnapshotIndex, rf.lastSnapshotTerm)
	rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(rf.generateState(), args.Data)
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
		Offset:            0,
		Done:              0,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	for {
		ok := rf.peers[server].Call("Raft.InstallSnapshotRpc", &args, &reply)
		if ok {
			break
		}
		time.Sleep(HeartBeatInterval * time.Millisecond)
	}
	rf.mu.Lock()
	if args.Term != rf.currentTerm || rf.state != LEADER {
		DPrintf(-1, "[%d] snap error because term are not same or not leader", rf.me)
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.resetRandomInterval()
		rf.mu.Unlock()
		rf.persist()
		return
	}
	DPrintf(-1, "[%d] rpc snap success in server %d", rf.me, server)

	// server accepted
	if rf.nextIndex[server] < args.LastIncludedIndex+1 {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
	if rf.matchIdex[server] < args.LastIncludedIndex {
		rf.matchIdex[server] = args.LastIncludedIndex
	}
	rf.mu.Unlock()
}

func (rf *Raft) ShouldSnapshot(limit int) bool {
	//DPrintf(-1, "[%d] state size = %d", rf.me, rf.persister.RaftStateSize())
	return rf.persister.RaftStateSize() >= limit
}

func (rf *Raft) ConverToFollower(term int) {
	rf.mu.Lock()

	DPrintf(0, "[%d] become follower in term %d", rf.me, term)
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1

	rf.mu.Unlock()
	rf.persist() // 持久化

	rf.resetRandomInterval()
}

func (rf *Raft) ConverToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = LEADER
	rf.available = true
	DPrintf(-1, "[%d] become leader (currentTerm=%d)\n", rf.me, rf.currentTerm)
	rf.matchIdex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	deadlock.Opts.Disable = false
	deadlock.Opts.DeadlockTimeout = time.Second * 3
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.electionFlag = make(chan bool)
	rf.state = FOLLOWER
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	rf.log = append(rf.log, Log{Term: 0, Idx: 0})
	rf.mu.Unlock()
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Tick()
	rf.resetRandomInterval()

	return rf
}
