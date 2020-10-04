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
}

type AskForTaskReply struct {
	// master对worker的回复
	// 要处理的文件名
	Filename string
	// reduce Task 数量
	NReduce int
	// 对应的 Map Task number
	MapId int
	// 如果是 Reduce Task,就返回 Reduce Task Number
	ReduceId int
	// 任务类型: Reduce or Map
	Task TaskType
}

// 位置信息
type PositionSizeArgs struct {
	// reduce id 对应文件名
	Location string
	ReduceID int
	// 对应reduce id
}

type PositionSizeReply struct {
	Flag bool
}

// reduce worker向master请求对应文件名
type AskForPositionArgs struct {
	ReduceId int
}

type AskForPositionReply struct {
	Positions []string
	ReduceId  int
}

type NotifyMapFinishArgs struct {
	MapId int
}

type NotifyMapFinishReply struct {
}

type NotifyReduceFinishArgs struct {
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
