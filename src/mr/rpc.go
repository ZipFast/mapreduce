package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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
type AskForTaskArgs struct {
	Flag bool
}

type AskForTaskReply struct {
	Filename string
	NReduce  int
	MapId    int
	ReduceId int
	Task     TaskType
}

type PositionSizeArgs struct {
	Position string
	Size     int64
	ReduceId int
}

type PositionSizeReply struct {
	Flag bool
}

type AskForPositionArgs struct {
	ReduceId int
}

type AskForPositionReply struct {
	Positions []string
	ReduceId  int
}

type NotifyMapFinishArgs struct {
	Flag  bool
	MapId int
}

type NotifyMapFinishReply struct {
}

type NotifyReduceFinishArgs struct {
	Flag     bool
	ReduceId int
}

type NotifyReduceFinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
