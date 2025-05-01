package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

func readFile(filename string) string {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	f.Close()
	return string(content)
}

func writeFile(filename string, content string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	_, err = f.WriteString(content)
	if err != nil {
		log.Fatalf("cannot write %v", filename)
	}
	f.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	var workerId int
	var nReduce int
	registerArgs := RegisterWorkerArgs{}
	registerReply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &registerArgs, &registerReply)
	if !ok {
		log.Fatal("RegisterWorker failed")
	}
	workerId = registerReply.WorkerId
	nReduce = registerReply.NReduce

	for {
		var task Task
		requestTaskArgs := &RequestTaskArgs{
			WorkerId: workerId,
		}
		requestTaskReply := &RequestTaskReply{}
		ok := call("Coordinator.RequestTask", requestTaskArgs, requestTaskReply)
		if !ok {
			log.Fatal("RequestTask failed")
		}

		task = requestTaskReply.Task

		if task.State != TaskInProgress || task.AssignedTo != workerId {
			time.Sleep(1 * time.Second)
			continue
		}

		switch task.Type {
		case MapTask:
			intermediate := make(map[int][]KeyValue)
			for _, f := range task.Files {
				content := readFile(f)

				mapfResults := mapf(f, content)
				for _, kv := range mapfResults {
					i := ihash(kv.Key) % nReduce
					intermediate[i] = append(intermediate[i], kv)
				}
			}

			intermediateFiles := make(map[int]string)
			for i, kvs := range intermediate {
				intermediateFile := fmt.Sprintf("mr-%d-%d", task.ID, i)
				intermediateFiles[i] = intermediateFile
				intermediateFileContent := ""
				for _, kv := range kvs {
					intermediateFileContent += fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
				}
				writeFile(intermediateFile, intermediateFileContent)
			}

			reportTaskArgs := &ReportTaskArgs{
				WorkerId:     workerId,
				TaskId:       task.ID,
				TaskType:     MapTask,
				TaskState:    TaskDone,
				Intermediate: intermediateFiles,
			}
			reportTaskReply := &ReportTaskReply{}

			ok := call("Coordinator.ReportTask", reportTaskArgs, reportTaskReply)
			if !ok {
				log.Fatal("ReportTask failed")
			}
			break
		case ReduceTask:
			intermediateFiles := task.Files
			keyToValues := make(map[string][]string)
			for _, f := range intermediateFiles {
				intermediateFileContent := readFile(f)
				for _, line := range strings.Split(intermediateFileContent, "\n") {
					kv := strings.Split(line, " ")
					if len(kv) != 2 {
						continue
					}
					if _, ok := keyToValues[kv[0]]; !ok {
						keyToValues[kv[0]] = []string{}
					}
					keyToValues[kv[0]] = append(keyToValues[kv[0]], kv[1])
				}
			}

			reduceResult := ""
			for k, v := range keyToValues {
				reduceResult += fmt.Sprintf("%v %v\n", k, reducef(k, v))
			}

			reduceResultFile := fmt.Sprintf("mr-out-%d", task.ID)
			writeFile(reduceResultFile, reduceResult)

			writeFile(fmt.Sprintf("reduce-%d", task.ID), reduceResult)

			reportTaskArgs := &ReportTaskArgs{
				WorkerId:  workerId,
				TaskId:    task.ID,
				TaskType:  ReduceTask,
				TaskState: TaskDone,
			}
			reportTaskReply := &ReportTaskReply{}
			ok := call("Coordinator.ReportTask", reportTaskArgs, reportTaskReply)
			if !ok {
				log.Fatal("ReportTask failed")
			}
			break
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
