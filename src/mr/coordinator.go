package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex // refer to https://go.dev/tour/concurrency/9

// refer to the usage of iota https://www.gopherguides.com/articles/how-to-use-iota-in-golang
const (
	Map State = iota
	Reduce
	Exit
	Wait
) 
const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)
type State int
type CoordinatorTaskStatus int

type Coordinator struct {
	// Your definitions here.
	CoordinatorPhase State
	TaskQueue chan *Task// channel
	TaskMeta map[int] *CoordinatorTasks // key type is all int, value type is different, defined in CoordinatorTasks
	NReduce int
	InputFiles []string
	Intermediates [][]string //an array of arrany, an intermediates storing each task info
}

type CoordinatorTasks struct {
	TaskStatus CoordinatorTaskStatus
	StartTime time.Time
	TaskReference *Task
}

type Task struct {
	Input string
	Output string
	TaskState State
	TaskNumber int
	NReducer int
	Intermediates []string
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// refer to make function here: https://www.educative.io/answers/what-is-golang-function-maket-type-size-integertype-type
	// make(type, size), with size, it assign memory on heap 
	c := Coordinator{
		TaskQueue: make(chan *Task, max(nReduce, len(files))),
		TaskMeta: make(map[int]*CoordinatorTasks),
		CoordinatorPhase: Map, // set it to 0 when it first start
		NReduce: nReduce,
		InputFiles: files,
		Intermediates: make([][]string, nReduce),
	}

	// Your code here.
	// create map task
	c.createMapTask()

	c.server()
    // start a go routine to check timeout
	go c.catchTimeOut()
	return &c
}

// arg before function meaning this function is a method of type Coordinator, we operate it on object c so we can call c.createMapTask
// reference: https://www.linkedin.com/pulse/go-things-parenthesis-before-function-name-shivam-chaurasia/
func (c *Coordinator) createMapTask(){
	// iterate filesname array, create a Task for each.
	for idx, filename := range c.InputFiles{
		println(idx, filename)
		taskMeta := Task{
			Input: filename,
			TaskState: Map,
			NReducer: c.NReduce,
			TaskNumber: idx,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTasks{
			TaskStatus: Idle,
			TaskReference: &taskMeta,
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) catchTimeOut(){
	// infinite loop -- for without condition
	for {
		time.Sleep(5 * time.Second)
		// mu.Lock()
		if c.CoordinatorPhase == Exit {
			// mu.Unlock()
			return
		}
		for _, CoordinatorTask := range c.TaskMeta {
			if CoordinatorTask.TaskStatus == InProgress && time.Now().Sub(CoordinatorTask.StartTime) > 10*time.Second {
				c.TaskQueue <- CoordinatorTask.TaskReference
				CoordinatorTask.TaskStatus = Idle
			}
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.CoordinatorPhase == Exit
	return ret
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func max(a int, b int) int {
	if a > b{
		return a
	}
	return b
}
