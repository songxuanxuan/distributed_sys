package main

import (
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strings"
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

func decrease() {

}

func main() {
	sort_index()
	//fmt.Println(_ihash("bads"))
}
