package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// ask master for task

	for true {
		args := AskForTaskArgs{}
		args.Flag = true
		reply := AskForTaskReply{}
		flag := call("Master.AskForTask", &args, &reply)
		if !flag {
			break
		}

		task := reply.Task
		fmt.Println(task)
		if task == Map {
			fmt.Printf("start mapping task %d\n", reply.MapId)
			intermediate := []KeyValue{}
			filename := reply.Filename
			nReduce := reply.NReduce
			mapid := reply.MapId
			file, err := os.Open(filename)
			if err != nil {
				log.Panicf("cannot open 1\n")
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Panicf("cannot read 2\n")
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			var reduceid int
			for _, kv := range intermediate {
				key := kv.Key
				reduceid = ihash(key) % nReduce
				oname := fmt.Sprintf("mr-%d-%d", mapid, reduceid)
				_, err := os.Stat(oname)
				isExsist := func(path string) bool {
					if err != nil {
						if os.IsExist(err) {
							return true
						}
						return false
					}
					return true
				}(oname)
				var file *os.File
				if !isExsist {
					file, _ = os.Create(oname)
				} else {
					file, _ = os.OpenFile(oname, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
				}
				enc := json.NewEncoder(file)
				enc.Encode(&kv)
				file.Close()
				posArgs := PositionSizeArgs{}
				posArgs.Position = oname
				posArgs.ReduceId = reduceid
				posReply := PositionSizeReply{}
				call("Master.PositionSize", &posArgs, &posReply)
			}

			notifyMapFinishArgs := NotifyMapFinishArgs{}
			notifyMapFinishArgs.Flag = true
			notifyMapFinishArgs.MapId = mapid
			call("Master.NotifyMapFinish", &notifyMapFinishArgs, nil)
		} else if task == Reduce {
			fmt.Printf("start reduce task %d\n", reply.ReduceId)
			reduceId := reply.ReduceId
			askForPostionArgs := AskForPositionArgs{}
			askForPostionArgs.ReduceId = reduceId
			askForPositionReply := AskForPositionReply{}
			call("Master.AskForPosition", &askForPostionArgs, &askForPositionReply)
			positions := askForPositionReply.Positions
			buffer := []KeyValue{}
			for _, position := range positions {
				file, err := os.Open(position)
				if err != nil {
					log.Panicf("cannot open 3 %s\n", position)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					buffer = append(buffer, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(buffer))
			oname := fmt.Sprintf("mr-out-%d", reduceId)
			fmt.Println("reduce file " + oname)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(buffer) {
				j := i + 1
				for j < len(buffer) && buffer[j].Key == buffer[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, buffer[k].Value)
				}
				output := reducef(buffer[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", buffer[i].Key, output)
				i = j
			}
			ofile.Close()
			fmt.Printf("Reduce Task %d complete\n", reduceId)
			reduceFinishArgs := NotifyReduceFinishArgs{}
			reduceFinishReply := NotifyReduceFinishReply{}
			reduceFinishArgs.Flag = true
			reduceFinishArgs.ReduceId = reduceId
			call("Master.NotifyReduceFinish", &reduceFinishArgs, &reduceFinishReply)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
