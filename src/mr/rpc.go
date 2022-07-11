package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
type FileReply struct {
	Name    string
	NReduce int
	Wid     int
}
type TaskArgs struct {
	Wid      int
	UniqueId int
}
type TaskReply struct {
	Wid      int
	Stage    int
	UniqueId int
}
type InterFile struct {
	Name       string
	ReducerIdx int
	UniqueId   int
}
type InterFilesArgs struct {
	Wid        int
	UniqueId   int
	InterFiles []InterFile
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket Name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
