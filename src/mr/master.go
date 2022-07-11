package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const TIMEVAL = 10 // 倒计数
const (
	MAP int = iota
	REDUCE
	FINISHED
	FILES_ALLOCATED
)

type task struct {
	tid       int
	filename  string
	allocated bool
	completed bool
}

type worker struct {
	idle     bool
	uniqueId int
	fileId   int // 分配给这个worker的文件id
	timeVal  int // 倒计时
}

type Master struct {
	// Your definitions here.
	files           []task
	workers         []worker
	workersMap      map[int]int
	nWorker         int
	mapper          []int
	reducer         []int
	interfiles      []InterFile //todo
	idle            int
	inProgress      int
	nCompleted      int
	nReduce         int
	nMap            int
	reduceFinished  []bool
	stage           int
	nInterCompleted int
}

// FileAllocator
//Your code here -- RPC handlers for the worker to call.
//File alloc handler
//
func (m *Master) FileAllocator(args *TaskArgs, reply *FileReply) error {

	if m.nCompleted >= len(m.files) {
		return nil
	}
	for i, f := range m.files {
		//fmt.Printf("nCompleted=%v,filename:%v,completed:%v\n",m.nCompleted,f.filename,f.completed)
		if !f.allocated {

			reply.Name = f.filename
			reply.NReduce = m.nReduce
			m.workers[args.Wid].fileId = i // 记录分配的文件给workers
			m.files[i].tid = args.Wid      // 没用
			m.files[i].allocated = true
			m.files[i].completed = false
			return nil
		}
	}
	// 循环结束说明文件全部被分配
	m.stage = FILES_ALLOCATED
	return nil
}

// 顺序分配worker
func (m *Master) allocWorker(wkr worker) int {

	for i, w := range m.workers {
		if m.stage == REDUCE && i >= m.nReduce {
			break
		}
		if w.uniqueId == 0 {
			m.workers[i] = wkr
			return i
		}
	}
	return -1
}

//TaskAllocator reply return map or reduce
func (m *Master) TaskAllocator(args *TaskArgs, reply *TaskReply) error {
	// 随机分配一个唯一id
	rand.Seed(time.Now().UnixNano())
	randomNum := rand.Intn(899) + 100
	wkr := worker{idle: false, uniqueId: randomNum, timeVal: TIMEVAL}
	wid := m.allocWorker(wkr)
	uid := randomNum

	if m.nCompleted < len(m.files) {
		// map阶段
		if m.stage == FILES_ALLOCATED {
			// 文件全部分配完了
			reply.Stage = FILES_ALLOCATED
			return nil
		} else {
			//m.stage = MAP	//状态值修改放在分配, 或者完成时.
			reply.Stage = 0
			reply.UniqueId = uid
			reply.Wid = wid
			//m.mapper = append(m.mapper, reply.Wid)
		}

	} else if m.nInterCompleted < m.nReduce {
		// reduce 阶段
		//m.stage = REDUCE
		reply.Stage = 1
		reply.UniqueId = uid
		reply.Wid = wid
		//m.reducer = append(m.reducer, reply.Wid)
		//fmt.Printf("log: reduce wid %v\n", wid)

	} else {
		//m.stage = FINISHED
		reply.Stage = 2
	}
	return nil
}

// ReceiveInterFiles 服务器接受中间文件名
func (m *Master) ReceiveInterFiles(args *InterFilesArgs, reply *TaskReply) error {
	if m.workers[args.Wid].uniqueId != args.UniqueId {
		return nil
	}
	m.interfiles = append(m.interfiles, args.InterFiles...)
	fid := m.workers[args.Wid].fileId
	m.files[fid].completed = true
	m.nCompleted++

	// 初始化workers
	if m.nCompleted >= len(m.files) {
		for i, _ := range m.workers {
			m.workers[i].uniqueId = 0
		}
		m.stage = REDUCE
	}
	return nil
}

// InterFileAllocator 中间文件分配
func (m *Master) InterFileAllocator(args *TaskArgs, reply *InterFilesArgs) error {
	for _, interFile := range m.interfiles[:] {
		if args.Wid == interFile.ReducerIdx {
			reply.InterFiles = append(reply.InterFiles, interFile)
		}
	}
	return nil
}

// InterFileReporter  中间文件完成报告
func (m *Master) InterFileReporter(args *TaskArgs, reply *InterFilesArgs) error {
	//fmt.Printf("log: stage %v, wid %v, uid %v, nic %v\n", m.stage, args.Wid, args.UniqueId, m.nInterCompleted)
	m.reduceFinished[args.Wid] = true
	m.nInterCompleted++
	if m.nInterCompleted >= m.nReduce {
		m.stage = FINISHED
	}
	return nil
}

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 100
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go m.CheckValid()
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	if m.stage == FINISHED {
		ret = true
	}
	return ret
}

// CheckValid 定时检查worker的工作情况, 10秒后无应答视为放弃.
func (m *Master) CheckValid() {
	for {
		time.Sleep(time.Second)
		for i, w := range m.workers {
			if w.uniqueId == 0 {
				continue
			}

			m.workers[i].timeVal--
			if m.stage == MAP || m.stage == FILES_ALLOCATED {

				if m.workers[i].timeVal == 0 && !m.files[w.fileId].completed {
					m.workers[i].uniqueId = 0
					m.files[w.fileId].allocated = false
					m.stage = MAP
					fmt.Printf("log: kick out %v free file %v\n", w.uniqueId, m.files[w.fileId].filename)
				}
			} else {
				if m.workers[i].timeVal == 0 && !m.reduceFinished[i] {
					m.workers[i].uniqueId = 0
					fmt.Printf("log: kick out %v free mr-out-%v \n", w.uniqueId, i)
				}
			}

		}
	}
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce, nCompleted: 0, nMap: nReduce * 2}
	m.workers = make([]worker, 20)
	m.workersMap = make(map[int]int)
	m.reduceFinished = make([]bool, nReduce)
	//m.reducer = make([]int,m.nReduce)
	//m.mapper = make([]int,m.nMap)
	// Your code here.
	for _, f := range files[:] {
		//fmt.Println(f)
		m.files = append(m.files, task{filename: f})
	}
	m.idle = len(m.files)
	os.RemoveAll(Project_path + "/intermediate")
	m.server()
	return &m
}
