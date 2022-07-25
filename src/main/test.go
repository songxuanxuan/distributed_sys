package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

func _ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func rename() {
	f, _ := os.OpenFile("mytt.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	os.Rename(f.Name(), strings.Split(f.Name(), ".")[0])
}
func sort_index() {
	s := []int{1, 3, 2, 3, 4, 5, 6}
	sc := make([]int, len(s))
	copy(sc, s)
	sort.Ints(sc)
	log.Printf("%v", s)
}

type xx struct {
	inter interface{}
}

type Inter struct {
	inx int
	iny int
}

func interfaceTest() {
	x := Inter{inx: 123, iny: 456}
	a := xx{inter: x}
	i := a.inter.(Inter).inx
	fmt.Printf("%v", i)

}

func getIndexTest() {
	str := "apple apple"
	i := strings.LastIndex(str, "app")
	end := strings.Index(str[i:], "l") + i + 1

	fmt.Printf("%v", str[i:end])
}

func chanTestAux(ch chan int, i int) {
	go func() {
		time.Sleep(time.Second)
		select {
		case <-ch:
			fmt.Printf("client receive chan time out \n")
			return
		case <-time.After(time.Second):
			return
		}

	}()
	ch <- i
	ch <- 2

}
func chanTest() {
	ch := make(chan int)
	go chanTestAux(ch, 1)
	j := <-ch
	fmt.Printf("%d \n", j)
	time.Sleep(3 * time.Second)
	//k := <-ch
	//fmt.Printf("%d \n", k)
	//m := <-ch
	//fmt.Printf("%d \n", m)
}

func testAppend() {
	x := []int{1, 2, 3, 4}
	y := []int{33, 44, 55}
	x = append(x[:3], y...)
	fmt.Printf("%v", x)
}

func main() {
	testAppend()
	//fmt.Println(_ihash("bads"))
}
