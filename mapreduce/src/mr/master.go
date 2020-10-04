package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type State int

const (
	idle State = iota
	running
	completed
	undefine
)

type Master struct {
	// Your definitions here.
	// 存放要处理的文件名
	filenames []string
	//记录当前各个Map Task的状态
	mapState []State
	//记录当前各个Reduce Task的状态
	reduceState []State
	// 在map函阶段传上来的位置信息，实际是一个文件名
	locations map[int][]string
	// 由client确定的 Reduce Task的数量
	nReduce int
	// 锁
	locker sync.Locker
	// 信号量
	cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.
// 请求任务的RPC，
func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	// 首先判断当前是否所有的任务都执行完了, 说明此时Master已结束，就直接返回一个error
	if m.Done() {
		return errors.New("Master exit")
	}
	fmt.Printf("Recive ask for task rpc call\n")
	// 要访问Master中的共享资源，需要加锁
	m.cond.L.Lock()
	// 首先遍历 Map Task的状态
	for i, state := range m.mapState {
		// 如果状态是空闲,那么可以分配这个Map Task
		if state == idle {
			// 分配文件
			fmt.Printf("Map task %d is idle, worker can work on that\n", i)
			reply.Filename = m.filenames[i]
			reply.NReduce = m.nReduce
			// 由于是Map Task， 分配 Map Task Number
			reply.MapId = i
			reply.Task = Map
			//将对应的 Map Task的状态修改为运行中
			m.mapState[i] = running
			//解锁
			m.cond.L.Unlock()
			return nil
		}
	}
	// 如果所有的map Task都不空闲，那么尝试分配 Reduce Task
	// if all the task are doing
	for i, state := range m.reduceState {
		// 如果状态空闲，那么可以分配这个 Reduce Task
		if state == idle {
			reply.Task = Reduce
			reply.ReduceId = i
			m.reduceState[i] = running
			m.cond.L.Unlock()
			return nil
		} else if state == undefine {
			// 如果此时这个 Map Task还没有可以处理的数据(之前的Map Task还没有处理完)
			fmt.Printf("reduce task %d is undefine, it should wait for map task complete\n", reply.ReduceId)
			//就等待
			m.cond.Wait()
		}
	}
	m.cond.L.Unlock()
	return nil
}

func (m *Master) NotifyMapFinish(args *NotifyMapFinishArgs, reply *NotifyMapFinishReply) error {
	m.cond.L.Lock()
	// 任务完成后，对应的map task state改为complete
	m.mapState[args.MapId] = completed
	for i, _ := range m.reduceState {
		m.reduceState[i] = idle
		m.cond.Signal()
	}
	m.cond.L.Unlock()
	return nil
}

func (m *Master) NotifyReduceFinish(args *NotifyReduceFinishArgs, reply *NotifyReduceFinishReply) error {
	m.cond.L.Lock()
	m.reduceState[args.ReduceId] = completed
	m.cond.L.Unlock()
	return nil
}

// 这个函数是 worker向 master传递位置信息
func (m *Master) PositionSize(args *PositionSizeArgs, reply *PositionSizeReply) error {
	// lock
	m.cond.L.Lock()
	// 获得对应的reduce id
	// 此时对应的Reduce Task已经可以开始运行
	reduceid := args.ReduceID
	m.locations[reduceid] = append(m.locations[reduceid], args.Location)
	// 唤醒正在等待的执行 reduce 任务的worker
	m.cond.L.Unlock()
	return nil
}

func (m *Master) AskForPosition(args *AskForPositionArgs, reply *AskForPositionReply) error {
	m.cond.L.Lock()
	reduceId := args.ReduceId
	reply.Positions = m.locations[reduceId]
	m.cond.L.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.cond.L.Lock()
	for _, mState := range m.mapState {
		if mState != completed {
			m.cond.L.Unlock()
			return false
		}
	}

	for _, rState := range m.reduceState {
		if rState != completed {
			m.cond.L.Unlock()
			return false
		}
	}
	ret = true
	m.cond.L.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.filenames = files
	m.mapState = make([]State, len(m.filenames))
	for i := range m.mapState {
		m.mapState[i] = idle
	}
	m.reduceState = make([]State, nReduce)
	for i := range m.reduceState {
		m.reduceState[i] = undefine
	}
	m.locations = make(map[int][]string)
	m.locker = new(sync.Mutex)
	m.cond = sync.NewCond(m.locker)

	m.server()
	return &m
}
