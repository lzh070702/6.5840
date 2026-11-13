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

const TaskTimeout = 10 * time.Second

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

type TaskState struct {
	Status    TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	phase       Phase
	files       []string
	nReduce     int
	mapTasks    []TaskState
	reduceTasks []TaskState
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.phase {
	case MapPhase:
		for id, state := range c.mapTasks {
			if state.Status == Idle || (time.Since(state.StartTime) > TaskTimeout && state.Status == InProgress) {
				c.mapTasks[id].Status = InProgress
				c.mapTasks[id].StartTime = time.Now()
				reply.Type = MapTask
				reply.ID = id
				reply.N = c.nReduce
				reply.File = c.files[id]
				return nil
			}
		}
		reply.Type = WaitTask
	case ReducePhase:
		for id, state := range c.reduceTasks {
			if state.Status == Idle || (time.Since(state.StartTime) > TaskTimeout && state.Status == InProgress) {
				c.reduceTasks[id].Status = InProgress
				c.reduceTasks[id].StartTime = time.Now()
				reply.Type = ReduceTask
				reply.ID = id
				reply.N = len(c.files)
				reply.File = ""
				return nil
			}
		}
		reply.Type = WaitTask
	case AllDone:
		reply.Type = ExitTask
	}
	return nil
}

func (c *Coordinator) ReportDone(args *ReportDoneArgs, reply *ReportDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case MapTask:
		c.mapTasks[args.ID].Status = Done
		allDone := true
		for _, ts := range c.mapTasks {
			if ts.Status != Done {
				allDone = false
				break
			}
		}
		if allDone && c.phase == MapPhase {
			c.phase = ReducePhase
		}
	case ReduceTask:
		c.reduceTasks[args.ID].Status = Done
		allDone := true
		for _, ts := range c.reduceTasks {
			if ts.Status != Done {
				allDone = false
				break
			}
		}
		if allDone && c.phase == ReducePhase {
			c.phase = AllDone
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.phase == AllDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:       MapPhase,
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]TaskState, len(files)),
		reduceTasks: make([]TaskState, nReduce),
	}

	// Your code here.

	c.server(sockname)
	return &c
}
