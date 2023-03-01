package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
)

/*
orders:  Worker() -> askForTask() -> call() -> coordinatorSock() from rpc.go
*/

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


// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
    for {
		task := askForTask()
		fmt.Println("task: ", task)
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// ask for task from master
func askForTask() Task {

	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.AssignTask", &args, &reply)
	return reply
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {

	content, err := ioutil.ReadFile(task.Input)

	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}

	// parse the content to map function that emits an array of key value pair
	// then store them in buffer,
	intermediates := mapf(task.Input, string(content))
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	println("buffer ", buffer)
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}
	println("mapOutput ", mapOutput)
	task.Intermediates = mapOutput
	TaskComplete(task)
}

func writeToLocalFile(taskNumber int, i int, buffer *[]KeyValue) string {
	// find current directory
	dir, _:= os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create a temp file", err)
	}
	encode := json.NewEncoder(tempFile)
	for _, kv := range *buffer {
		if err := encode.Encode(&kv); err != nil {
			log.Fatal("Failed to write key value pair")
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", taskNumber, i)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func TaskComplete(task *Task) {
	// send the intermediates to Coordinator through RPC
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", &task, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func call(rpcname string, args interface{}, reply interface{}) bool {
	// client, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	
	if err != nil {
		log.Fatal("dialing:", err)
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

