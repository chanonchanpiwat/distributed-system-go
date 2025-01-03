package main

//
// start the master process, which is implemented
// in mr "github.com/chanonchanpiwat/distributed-system-go/src/mr"/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"
	mr "github.com/chanonchanpiwat/distributed-system-go/src/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
