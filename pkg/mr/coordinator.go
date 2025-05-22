package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type status int

const (
	Idle status = iota
	Running
	Done
)

type JobType int

const (
	MapJob JobType = iota
	ReduceJob
)

type JobInfo struct {
	TypeOfJob     JobType
	ReduceJobName int
	JobFiles      []string
}

type FinishedWork struct {
	JobName     string
	OutputFiles map[int][]string
}

type FinishedReduceJob struct {
	ReduceJobName int
}

type Coordinator struct {
	mapJobs           map[string]status
	reduceJobs        map[int]status
	requestWork       chan chan JobInfo
	workDone          chan FinishedWork
	reduceWorkDone    chan FinishedReduceJob
	timeoutCheck      chan string
	intermediateFiles map[int][]string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestJob(args int, reply *JobInfo) error {
	nextJob := make(chan JobInfo, 1)
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

func (c *Coordinator) ReduceJobDone(input *FinishedReduceJob, reply *string) error {
	c.reduceWorkDone <- *input
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

func (c *Coordinator) getIdleJob(getNextJob chan JobInfo) {
	if !c.mapFinished() {
		newJobInfo := JobInfo{TypeOfJob: MapJob}
		for mapJob, status := range c.mapJobs {
			if status == Idle {
				c.mapJobs[mapJob] = Running
				go func(timeoutChannel chan string) {
					time.Sleep(time.Second * 10)
					timeoutChannel <- mapJob
				}(c.timeoutCheck)
				newJobInfo.JobFiles = append(newJobInfo.JobFiles, mapJob)
				getNextJob <- newJobInfo
				return
			}
		}
		newJobInfo.JobFiles = append(newJobInfo.JobFiles, "Not Done")
		getNextJob <- newJobInfo
	} else if !c.reduceFinished() {
		newJobInfo := JobInfo{TypeOfJob: ReduceJob}
		for reduceJob, reduceStatus := range c.reduceJobs {
			if reduceStatus == Idle {
				c.reduceJobs[reduceJob] = Running
				newJobInfo.JobFiles = append(newJobInfo.JobFiles, c.intermediateFiles[reduceJob]...)
				newJobInfo.ReduceJobName = reduceJob
				getNextJob <- newJobInfo
				return
			}
		}
		newJobInfo.JobFiles = append(newJobInfo.JobFiles, "Not Done")
		getNextJob <- newJobInfo
	} else {
		getNextJob <- JobInfo{TypeOfJob: MapJob, JobFiles: []string{"Done"}}
	}
}

func (c *Coordinator) recordFinishedJob(job FinishedWork) {
	jobStatus, ok := c.mapJobs[job.JobName]

	if ok && (jobStatus != Done) {
		c.mapJobs[job.JobName] = Done
		for bucket, files := range job.OutputFiles {
			_, foundBucket := c.intermediateFiles[bucket]
			if foundBucket {
				c.intermediateFiles[bucket] = append(c.intermediateFiles[bucket], files...)
			} else {
				c.intermediateFiles[bucket] = files
			}
		}
	}
}

func (c *Coordinator) recordFinishedReduceJob(job FinishedReduceJob) {
	jobStatus, ok := c.reduceJobs[job.ReduceJobName]

	if ok && (jobStatus != Done) {
		c.reduceJobs[job.ReduceJobName] = Done
	}
}

func (c *Coordinator) checkTimeoutFile(timeoutName string) {
	mapStatus, ok := c.mapJobs[timeoutName]
	if ok && (mapStatus != Done) {
		c.mapJobs[timeoutName] = Idle
	}
}

func (c *Coordinator) eventLoop() {
	for {
		select {
		case getWork := <-c.requestWork:
			c.getIdleJob(getWork)
		case jobComplete := <-c.workDone:
			c.recordFinishedJob(jobComplete)
		case reduceJobComplete := <-c.reduceWorkDone:
			c.recordFinishedReduceJob(reduceJobComplete)
		case timeoutName := <-c.timeoutCheck:
			c.checkTimeoutFile(timeoutName)
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
		requestWork:       make(chan chan JobInfo),
		workDone:          make(chan FinishedWork),
		reduceWorkDone:    make(chan FinishedReduceJob),
		timeoutCheck:      make(chan string),
		intermediateFiles: make(map[int][]string, nReduce),
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
