package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var Project_path = "/home/xuan/mit_distribution"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 相同的单词hash到同一个文件
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for { // 死循环
		// Your worker implementation here.
		workerReply := Call4Task()
		if workerReply.Stage == FILES_ALLOCATED || workerReply.Wid == -1 {
			time.Sleep(time.Second * 2)
		} else if workerReply.Stage == 2 {
			return
		} else if workerReply.Stage == 0 {
			//progress map
			reply := Call4File(workerReply.Wid)
			// 文件已经全部分配完成.
			if len(reply.Name) == 0 {
				continue
			}
			_ = os.Mkdir(Project_path+"/intermediate", 0777)
			// 创建临时文件池
			interFilesPool := make([]*os.File, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				interName := Project_path + "/intermediate/mr-" + strconv.Itoa(workerReply.Wid) + "-" + strconv.Itoa(i) + "." + strconv.Itoa(workerReply.UniqueId)
				intermediateFile, err := os.OpenFile(interName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					log.Fatalf("intermediate : cannot create/open %v", interName)
					log.Println(err.Error())
				}
				interFilesPool[i] = intermediateFile
			}
			file, err := os.Open(reply.Name)
			if err != nil {
				log.Fatalf("in map--cannot open:%v", reply.Name)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("in map--cannot read:%v", reply.Name)
			}
			file.Close()

			kva := mapf(reply.Name, string(content))
			interFilesReply := InterFilesArgs{Wid: workerReply.Wid, UniqueId: workerReply.UniqueId}
			for _, kv := range kva[:] {
				//
				//spread kv to intermediateFiles
				//
				idReduceInt := ihash(kv.Key) % reply.NReduce

				enc := json.NewEncoder(interFilesPool[idReduceInt])
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v : %v", kv.Key, kv.Value)
				}

			}
			// 传输该map生成的所有中间文件, 并设置该文件task为完成
			for i := 0; i < reply.NReduce; i++ {
				newName := strings.Split(interFilesPool[i].Name(), ".")[0]
				os.Rename(interFilesPool[i].Name(), newName)
				interFilesReply.InterFiles = append(interFilesReply.InterFiles, InterFile{newName, i, workerReply.UniqueId})
				err := interFilesPool[i].Close()
				if err != nil {
					return
				}
			}
			call("Master.ReceiveInterFiles", &interFilesReply, nil)
		} else {
			//progress reduce
			args := TaskArgs{Wid: workerReply.Wid}
			reply := InterFilesArgs{}
			call("Master.InterFileAllocator", &args, &reply)
			var intermediate []KeyValue
			for _, interFile := range reply.InterFiles {
				file, err := os.Open(interFile.Name)
				if err != nil {
					log.Fatalf("in reduce---cannot open %v\n", interFile.Name)
				}
				// 将临时文件转化为json格式, 即kv
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			// 按单词排序后, 把相同的合并数量
			sort.Sort(ByKey(intermediate))
			// todo 改成绝对路径
			tmpName := strconv.Itoa(workerReply.UniqueId)
			outName := "mr-out-" + strconv.Itoa(workerReply.Wid)
			outFile, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Fatalf("cannot open %v\n", tmpName)
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				// 把相同的词语放一起进行计算数量
				var values []string //todo
				for k := i; k < j; k++ {
					//add all amount of same key
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			os.Rename(outFile.Name(), outName)
			outFile.Close()
			call("Master.InterFileReporter", &args, nil)
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// Call4File
//call for a file task
func Call4File(wid int) FileReply {
	reply := FileReply{}
	args := TaskArgs{Wid: wid}
	call("Master.FileAllocator", &args, &reply)
	//fmt.Printf("log: reply.Name:%v\n",reply.Name)
	return reply
}

// Call4Task call for map or reduce task,true for map,false for reduce
func Call4Task() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Master.TaskAllocator", &args, &reply)
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
