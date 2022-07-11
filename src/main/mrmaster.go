package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import "../mr"

//import "../mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	// 开线程监听workers的rpc调用
	m := mr.MakeMaster(os.Args[1:], 10)

	for m.Done() == false {
		time.Sleep(time.Second)
	}
	//fmt.Printf("log: mian exited..\n")
	time.Sleep(time.Second)
}
