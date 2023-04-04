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

func max(a int, b int) int {
	if a > b{
		return a
	}
	return b
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
// read rpc package here: https://pkg.go.dev/net/rpc  see example code from onward #The server calls (for HTTP service)
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)  // In real world this would be a TCP connection, something like l, e := net.Listen("tcp", ":1234")
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

func (c *Coordinator) TaskCompleted(task *Task, reply * ExampleReply) error {
	mu.Lock()
	// Once it's assigned to Map, at this point the taskState of a task is still map. Assigned in createMapTask
	// The task state is always the same as CoordinatorPhase, b/c it stays in Map until all mapper done and then it will go to the next phase. See func processTaskResult. It calls allTaskDone before next state
	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	mu.Unlock()
	defer c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		for reduceTaskId, filePath := range task.Intermediates{
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		if c.allTaskDone(){
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone(){
			c.CoordinatorPhase = Exit
		}
	}
}

func (c *Coordinator) allTaskDone()bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}

	return true
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTasks)
	for idx, file := range c.Intermediates{
		taskMeta := Task {
			TaskState: Reduce,
			NReducer: c.NReduce,
			TaskNumber: idx,
			Intermediates: file,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTasks{
			TaskStatus: Idle,
			TaskReference: &taskMeta,
		}
	}
}