package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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
	// task type is either a Map or a Reduce
	Map TaskType = iota
	Reduce
)

// for sort
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 这个函数用来选择每个由Map函数生成的KeyValue pair对应的reduce task number
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
	// 一个worker首先需要向master请求工作，这个工作可以是Map，也可以是Reduce

	// 使用一个循环，防止worker运行完之后直接退出
	for true {
		// AskForTaskArgs是一个RPC，向master请求一个任务
		args := AskForTaskArgs{}
		// 得到RPC的回复
		reply := AskForTaskReply{}
		flag := call("Master.AskForTask", &args, &reply)
		// 如果Master已经退出，那么直接退出当前循环
		if !flag {
			break
		}
		// 首先得到任务类型
		task := reply.Task
		// 如果任务是Map Task
		if task == Map {
			fmt.Printf("start mapping task %d\n", reply.MapId)
			intermediate := []KeyValue{}
			// 得到文件名
			filename := reply.Filename
			// 得到NReduce
			nReduce := reply.NReduce
			// 得到map id
			mapid := reply.MapId
			// 打开文件
			fmt.Printf("Worker has recieved the rpc reply, the map task id is %d, file name is %s\n", mapid, filename)
			file, err := os.Open(filename)
			if err != nil {
				log.Panicf("cannot open 1\n")
			}
			// 读出文件内容
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Panicf("cannot read 2\n")
			}
			file.Close()
			// kva是mapf生成键值对，在wordCount任务下是list[<word, 1>]
			kva := mapf(filename, string(content))
			// 将键值对临时存储在内存中
			// 对内存中的键值进行按键排序
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			var reduceid int
			// 这里想要把这些key value pair存放到对应的文件中,文件名格式为 mr-X-Y
			// X is Map id, Y is Reduce id
			// filenames 的键是mr-X-Y的文件名， 值是对应的key value 数组
			filenames := make(map[string][]KeyValue)
			for _, kv := range intermediate {
				// 得到对应的key
				key := kv.Key
				// 求得对应的reduce id
				reduceid = ihash(key) % nReduce
				// 得到对应的文件名

				nameBuffer := bytes.Buffer{}
				nameBuffer.WriteString("mr-")
				nameBuffer.WriteString(strconv.Itoa(mapid))
				nameBuffer.WriteString("-")
				nameBuffer.WriteString(strconv.Itoa(reduceid))
				oname := nameBuffer.String()
				filenames[oname] = append(filenames[oname], kv)
			}
			for i := 0; i < nReduce; i++ {
				buffer := bytes.Buffer{}
				buffer.WriteString("mr-")
				buffer.WriteString(strconv.Itoa(mapid))
				buffer.WriteString("-")
				buffer.WriteString(strconv.Itoa(i))
				oname := buffer.String()
				posArgs := PositionSizeArgs{}
				posArgs.Location = oname
				posArgs.ReduceID = i
				posReply := PositionSizeReply{}
				call("Master.PositionSize", &posArgs, &posReply)
			}
			// 打开每个文件，使用json库写入这些键值
			for filename, kvs := range filenames {
				file, err := os.Create(filename)
				if err != nil {
					log.Panicf("cannot open %s\n", filename)
				}
				for _, kv := range kvs {
					enc := json.NewEncoder(file)
					jsonerr := enc.Encode(&kv)
					if jsonerr != nil {
						log.Panicf("json encode error\n")
					}
				}
				file.Close()
			}
			/*
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
			*/
			// 然后把对应的位置信息传递给master

			// 当这些都做完后，map task over
			// 告诉master任务完成
			notifyMapFinishArgs := NotifyMapFinishArgs{}
			notifyMapFinishArgs.MapId = mapid
			call("Master.NotifyMapFinish", &notifyMapFinishArgs, nil)
			// 如果是 reduce Task
		} else if task == Reduce {
			fmt.Printf("start reduce task %d\n", reply.ReduceId)
			reduceId := reply.ReduceId
			askForPostionArgs := AskForPositionArgs{}
			askForPostionArgs.ReduceId = reduceId
			askForPositionReply := AskForPositionReply{}
			fmt.Printf("now reduce task %d is asking for position\n", reduceId)
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
