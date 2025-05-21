package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

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

func outputFileResults(jobName string, kvResults []KeyValue) (generatedFiles []string) {
	generatedFiles = make([]string, 10)
	reduceTasks := make(map[int][]KeyValue, 10)
	for _, kvStruct := range kvResults {
		reduceBucket := ihash(kvStruct.Key) % 10
		_, ok := reduceTasks[reduceBucket]
		if ok {
			reduceTasks[reduceBucket] = append(reduceTasks[reduceBucket], kvStruct)
		} else {
			reduceTasks[reduceBucket] = []KeyValue{kvStruct}
		}
	}

	for key, value := range reduceTasks {
		b, err := json.Marshal(value)
		if err != nil {
			log.Fatal(err)
		}
		newFileName := "MR-" + jobName + "-" + strconv.Itoa(key)
		generatedFiles = append(generatedFiles, newFileName)
		err = os.WriteFile(newFileName, b, 0600)
		if err != nil {
			log.Fatal(err)
		}
	}

	return
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		var jobName string
		ok := call("RequestJob", 0, &jobName)
		if !ok {
			log.Fatal("Error with call RequestJob")
		}
		if jobName == "Done" {
			break
		}
		contents, err := os.ReadFile(jobName)
		if err != nil {
			log.Fatal(err)
		}

		kvResults := mapf(jobName, string(contents))
		intermediateFiles := outputFileResults(jobName, kvResults)
		args := FinishedWork{jobName: jobName, outputFiles: intermediateFiles}
		var temp string
		ok = call("JobDone", &args, &temp)
		if !ok {
			log.Fatal("Error with call JobDone")
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:2233")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
