package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type status int

const (
	Idle status = iota
	Running
	Done
)

type FinishedWork struct {
	JobName     string
	OutputFiles []string
}

type Coordinator struct {
	mapJobs           map[string]status
	reduceJobs        map[int]status
	requestWork       chan chan string
	workDone          chan FinishedWork
	intermediateFiles []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestJob(args int, reply *string) error {
	nextJob := make(chan string, 1)
	c.requestWork <- nextJob
	jobName := <-nextJob
	*reply = jobName

	return nil
}

func (c *Coordinator) JobDone(input *FinishedWork, reply *string) error {
	c.workDone <- *input
	*reply = "Unused"
	return nil
}

func (c *Coordinator) mapFinished() bool {
	for _, currentStatus := range c.mapJobs {
		if currentStatus != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceFinished() bool {
	for _, currentStatus := range c.reduceJobs {
		if currentStatus != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) getIdleJob(getNextJob chan string) {
	if !c.mapFinished() {
		for mapJob, status := range c.mapJobs {
			if status == Idle {
				getNextJob <- mapJob
				return
			}
		}
		getNextJob <- "Not Done"
	} else {
		getNextJob <- "Done"
	}
}

func (c *Coordinator) recordFinishedJob(job FinishedWork) {
	jobStatus, ok := c.mapJobs[job.jobName]

	if ok && (jobStatus != Done) {
		c.mapJobs[job.jobName] = Done
		c.intermediateFiles = append(c.intermediateFiles, job.outputFiles...)
	}
}

func (c *Coordinator) eventLoop() {
	for {
		select {
		case getWork := <-c.requestWork:
			c.getIdleJob(getWork)
		case jobComplete := <-c.workDone:
			c.recordFinishedJob(jobComplete)
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "127.0.0.1:2233")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.eventLoop()
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
		mapJobs:           make(map[string]status, len(files)),
		reduceJobs:        make(map[int]status, nReduce),
		requestWork:       make(chan chan string),
		workDone:          make(chan FinishedWork),
		intermediateFiles: make([]string, len(files)*nReduce),
	}

	for _, fileName := range files {
		c.mapJobs[fileName] = Idle
	}

	for i := 0; i < nReduce; i++ {
		c.reduceJobs[i] = Idle
	}

	c.server()
	return &c
}
