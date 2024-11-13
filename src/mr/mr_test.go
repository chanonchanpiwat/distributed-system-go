package mr

import (
	"fmt"
	"testing"
	"time"
)

func TestMapReduce(t *testing.T) {
	file := []string{"../main/pg-grimm.txt"}
	m := MakeMaster(file,1)
	

	time.Sleep(time.Second)
	Worker(Map, Reduce)

	fmt.Println("start")
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	fmt.Println("End")

}