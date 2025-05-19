package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type status int

const (
	Idle    status = iota
	Running status = iota
	Done    status
)

type jobResultInfo struct {
	currentStatus status
	outputFiles   []string
}

type Coordinator struct {
	mapJobs    map[string]jobResultInfo
	reduceJobs map[int]jobResultInfo
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) mapFinished() bool {
	for _, v := range c.mapJobs {
		if v.currentStatus != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceFinished() bool {
	for _, v := range c.reduceJobs {
		if v.currentStatus != Done {
			return false
		}
	}
	return true
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
	ret := c.mapFinished() && c.reduceFinished()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapJobs:    make(map[string]jobResultInfo, len(files)),
		reduceJobs: make(map[int]jobResultInfo, nReduce),
	}

	for _, fileName := range files {
		c.mapJobs[fileName] = jobResultInfo{currentStatus: Idle, outputFiles: []string{}}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceJobs[i] = jobResultInfo{currentStatus: Idle, outputFiles: []string{}}
	}

	c.server()
	return &c
}
