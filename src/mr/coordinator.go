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
	TaskMeta map[int] *CoordinatorTasks
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
// start a thread that listens for RPCs from worker.go
//
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
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
    // Continue from here, queue all the tasks

	c.server()
	return &c
}
