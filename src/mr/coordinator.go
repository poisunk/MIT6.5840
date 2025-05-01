package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	workers     int
	nReduce     int
	mapTasks    map[int]*Task
	reduceTasks map[int]*Task

	allMapTasksDone    bool
	allReduceTasksDone bool

	mu sync.Mutex

	timeout time.Duration
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers++
	reply.WorkerId = c.workers
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.allMapTasksDone {
		mapTask, has := c.assignTask(&c.mapTasks, args.WorkerId, &c.allMapTasksDone)
		if has {
			reply.Task = *mapTask
			return nil
		}
		return nil
	}

	reduceTask, has := c.assignTask(&c.reduceTasks, args.WorkerId, &c.allReduceTasksDone)
	if has {
		reply.Task = *reduceTask
		return nil
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		task := c.mapTasks[args.TaskId]
		if task.State != TaskInProgress || task.AssignedTo != args.WorkerId {
			return nil
		}

		c.updateTaskState(&c.mapTasks, args.TaskId, args.TaskState)

		for i, f := range args.Intermediate {
			reduceTask := c.reduceTasks[i]
			reduceTask.Files = append(reduceTask.Files, f)
			c.reduceTasks[i] = reduceTask
		}
		break
	case ReduceTask:
		task := c.reduceTasks[args.TaskId]
		if task.State != TaskInProgress || task.AssignedTo != args.WorkerId {
			return nil
		}

		c.updateTaskState(&c.reduceTasks, args.TaskId, args.TaskState)
		break
	default:
		return nil
	}

	return nil
}

func (c *Coordinator) assignTask(tasks *map[int]*Task, workerId int, allTasksDone *bool) (t *Task, has bool) {
	for _, t := range *tasks {
		if t.State == TaskWaiting || (t.State == TaskInProgress && time.Since(t.AssignedAt) > c.timeout) {
			t.State = TaskInProgress
			t.AssignedTo = workerId
			t.AssignedAt = time.Now()
			return t, true
		}
	}

	isAllDone := true
	for _, t := range *tasks {
		if t.State != TaskDone {
			isAllDone = false
			break
		}
	}
	*allTasksDone = isAllDone

	return nil, false
}

func (c *Coordinator) updateTaskState(tasks *map[int]*Task, taskId int, state string) {
	for _, t := range *tasks {
		if t.ID == taskId {
			t.State = state
			break
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.allMapTasksDone && c.allReduceTasksDone

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.workers = 0
	c.nReduce = nReduce
	c.timeout = 10 * time.Second

	c.allMapTasksDone = false
	c.allReduceTasksDone = false

	c.mapTasks = make(map[int]*Task)
	for _, f := range files {
		t := &Task{
			ID:    len(c.mapTasks),
			Type:  MapTask,
			Files: []string{f},
			State: TaskWaiting,
		}
		c.mapTasks[t.ID] = t
	}

	c.reduceTasks = make(map[int]*Task)
	for i := 0; i < nReduce; i++ {
		t := &Task{
			ID:    i,
			Type:  ReduceTask,
			Files: []string{},
			State: TaskWaiting,
		}
		c.reduceTasks[t.ID] = t
	}

	c.server()
	return &c
}
