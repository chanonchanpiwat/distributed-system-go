package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
)

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

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	// s := "/var/tmp/824-mr-"
	// s += strconv.Itoa(os.Getuid())

	// ADD ON: changing socket name "mr-socket"
	return "mr-socket"
}

func LogAndExit(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
