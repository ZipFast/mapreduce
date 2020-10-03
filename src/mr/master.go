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
	filenames   []string
	mapState    []State
	reduceState []State
	reduceId    int
	locations   []string
	nReduce     int
	locker      sync.Locker
	cond        *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	m.cond.L.Lock()
	for i, state := range m.mapState {
		if state == idle {
			reply.Filename = m.filenames[i]
			reply.NReduce = m.nReduce
			reply.MapId = i
			reply.Task = Map
			m.mapState[i] = running
			m.cond.L.Unlock()
			return nil
		}
	}
	// if all the task are doing
	for i, state := range m.reduceState {
		if state == idle {
			reply.Task = Reduce
			reply.ReduceId = i
			state = running
			m.cond.L.Unlock()
			return nil
		} else if state == undefine {
			fmt.Printf("reduce task %d is undefine, it should wait for map task complete\n", reply.ReduceId)
			m.cond.Wait()
		}
	}
	if m.Done() {
		m.cond.L.Unlock()
		return errors.New("Master exit")
	}
	m.cond.L.Unlock()
	return nil
}

func (m *Master) NotifyMapFinish(args *NotifyMapFinishArgs, reply *NotifyMapFinishReply) error {
	if args.Flag {
		m.cond.L.Lock()
		m.mapState[args.MapId] = completed
		m.cond.L.Unlock()
	}
	return nil
}

func (m *Master) NotifyReduceFinish(args *NotifyReduceFinishArgs, reply *NotifyReduceFinishReply) error {
	if args.Flag {
		m.cond.L.Lock()
		m.reduceState[args.ReduceId] = completed
		m.cond.L.Unlock()
	}
	return nil
}

func (m *Master) PositionSize(args *PositionSizeArgs, reply *PositionSizeReply) error {
	m.cond.L.Lock()
	reduceid := args.ReduceId
	fmt.Printf("reduce task %d is ready\n", reduceid)
	m.locations[reduceid] = args.Position
	m.reduceState[args.ReduceId] = idle
	m.cond.Signal()
	m.reduceId = reduceid
	reply.Flag = true
	m.cond.L.Unlock()
	return nil
}

func (m *Master) AskForPosition(args *AskForPositionArgs, reply *AskForPositionReply) error {
	m.cond.L.Lock()
	reduceId := args.ReduceId
	reply.Positions = append(reply.Positions, m.locations[reduceId])
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
	flag := true
	for _, mState := range m.mapState {
		if mState != completed {
			flag = false
			m.cond.L.Unlock()
			return false
		}
	}

	for _, rState := range m.reduceState {
		if rState != completed {
			flag = false
			m.cond.L.Unlock()
			return false
		}
	}
	if flag {
		ret = true
	}
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
	m.locations = make([]string, nReduce)
	m.locker = new(sync.Mutex)
	m.cond = sync.NewCond(m.locker)

	m.server()
	return &m
}
