package mr

import "log"
import "net"
import "os"
import "sync"
import "net/rpc"
import "net/http"

var mu sync.Mutex // refer to https://go.dev/tour/concurrency/9

const (
	Map State = iota
	Reduce
	Exit
	Wait
) // refer to the usage of iota https://www.gopherguides.com/articles/how-to-use-iota-in-golang

type State int

type Coordinator struct {
	// Your definitions here.
	CoordinatorPhase State

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

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
