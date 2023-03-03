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
	"sort"
)

/*
orders:  Worker() -> askForTask() -> call() -> coordinatorSock() from rpc.go
*/

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		case Reduce:
			reducer(&task, reducef)
		}
	}

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

func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	dir, _:= os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-temp-*")
	if err != nil {
		log.Fatal("Failed to reduce file", err)
	}

	// mrsequential.go
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	filename := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(),filename)
	task.Output = filename
	TaskComplete(task) // Continue from here! Call complete again when reduce is done
}

func readFromLocalFile(files []string) *[]KeyValue {
	keyValueList := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open to read file" + filepath, err)
		}
		doc := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := doc.Decode(&kv); err != nil {
				break
			}
			keyValueList = append(keyValueList, kv)
		}
		file.Close()
	}
	return &keyValueList
}