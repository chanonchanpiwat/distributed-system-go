package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var workWaitTime = 100 * time.Millisecond

func doTask(mapTask *MapTaskArg, reduceTask *ReduceTaskArg, taskId int, mapFunc MapFunc, reduceFunc ReduceFunc) {
	st := time.Now()
	var taskType TaskType

	if mapTask != nil {
		taskType = MapType
		err := doMap(mapTask.MapId, mapTask.FileName, mapTask.NumberOfReduce, mapFunc)
		LogAndExit(err)

	} else if reduceTask != nil {
		taskType = ReduceType
		err := doReduce(reduceTask.ReduceId, reduceTask.MapNum, reduceFunc)
		LogAndExit(err)

	}

	replyTask(ReplyTaskArg{taskId, taskType, int(time.Since(st))}, &ReplyTaskReply{})
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := os.Getuid()

	for {

		var task RequestTaskReply
		isSuccess := requestTask(RequestTaskArg{workerId}, &task)
		if !isSuccess {
			fmt.Println("Unable to contact master worker exit")
			return
		}

		if task.Proceed {
			doTask(task.MapTaskArg, task.ReduceTaskArg, task.TaskId, mapf, reducef)
		} else if task.Wait {
			time.Sleep(workWaitTime)
		} else if task.Exit {
			fmt.Println("All task is completed")
			return
		}

	}

}

func makeMapIntermediate(mapId int, reduceId int) string {
	return fmt.Sprintf("mapper-%d-%d", mapId, reduceId)
}

func makeOutPut(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}

type MapFunc = func(filename string, contents string) []KeyValue
type ReduceFunc = func(key string, values []string) string

func doMap(mapId int, fileName string, nReduce int, mapFunc MapFunc) error {
	/*
		input: fileName
		intermediate: mapper-{mapId}-{*} [* = reduceId]
	*/
	file, err := os.Open(fileName)
	LogAndExit(err)
	defer file.Close()

	bytes, err := io.ReadAll(file)
	LogAndExit(err)

	kvs := mapFunc(fileName, string(bytes))

	keyValuesByPartition := make(map[int][]KeyValue)
	for id := range nReduce {
		keyValuesByPartition[id] = []KeyValue{}
	}

	for _, kv := range kvs {
		partition := ihash(kv.Key) % nReduce
		keyValuesByPartition[partition] = append(keyValuesByPartition[partition], kv)
	}

	interMediateFileNames := make([]string, 0)
	for reduceId := range nReduce {
		interMediateFileNames = append(interMediateFileNames, makeMapIntermediate(mapId, reduceId))
	}

	for id, kvs := range keyValuesByPartition {
		file, err := os.Create(interMediateFileNames[id])
		LogAndExit(err)
		defer file.Close()

		content, err := json.Marshal(kvs)
		LogAndExit(err)

		_, err = file.Write(content)
		LogAndExit(err)
	}

	return nil
}

func doReduce(reduceId int, nMap int, reduceFunc ReduceFunc) error {
	/*
		intermediate: mapper-{*}-reduceId [*=MapId]
		output: mr-out-reduceId 
	*/
	intermediateFileNames := make([]string, 0)
	for mapId := range nMap {
		intermediateFileNames = append(intermediateFileNames, makeMapIntermediate(mapId, reduceId))
	}

	mapKeyToValue := make(map[string][]string)
	for _, fileName := range intermediateFileNames {
		file, err := os.Open(fileName)
		LogAndExit(err)

		content, err := io.ReadAll(file)
		LogAndExit(err)

		var inKv []KeyValue
		json.Unmarshal(content, &inKv)

		for _, kv := range inKv {
			mapKeyToValue[kv.Key] = append(mapKeyToValue[kv.Key], kv.Value)
		}
	}

	mapKeyToCount := make(map[string]string)
	for k, v := range mapKeyToValue {
		mapKeyToCount[k] = reduceFunc(k, v)
	}

	file, err := os.Create(makeOutPut(reduceId))
	LogAndExit(err)
	defer file.Close()

	w := bufio.NewWriter(file)

	for k, v := range mapKeyToCount {
		_, err := fmt.Fprintf(w, "%v %v\n", k, v)
		LogAndExit(err)
	}

	w.Flush()

	return nil
}

func requestTask(arg RequestTaskArg, reply *RequestTaskReply) bool {
	isSuccess := call("Master.RequestTask", arg, reply)
	return isSuccess
}

func replyTask(arg ReplyTaskArg, reply *ReplyTaskReply) bool {
	isSuccess := call("Master.ReplyTask", arg, reply)
	return isSuccess
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
