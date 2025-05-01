package mr

import "time"

const (
	MapTask    = "map"
	ReduceTask = "reduce"
)

const (
	TaskWaiting    = "waiting"
	TaskInProgress = "in progress"
	TaskDone       = "done"
)

type Task struct {
	ID         int
	Type       string
	Files      []string
	State      string
	AssignedTo int
	AssignedAt time.Time
}

// Register

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	WorkerId int
	NReduce  int
}

// RequestTask

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Task Task
}

// ReportTask

type ReportTaskArgs struct {
	WorkerId     int
	TaskId       int
	TaskType     string
	TaskState    string
	Intermediate map[int]string
}

type ReportTaskReply struct {
}
