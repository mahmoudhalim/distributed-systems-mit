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

type Status int

const (
	NotStarted Status = iota
	InProgress
	Completed
)

type TaskStatus struct {
	status    Status
	startTime time.Time
}

type Coordinator struct {
	mu sync.Mutex

	MapInputFiles  []string
	MapTasksStatus []TaskStatus
	CompletedMaps  int

	ReduceTasksStatus []TaskStatus
	CompletedReduces  int
	nReduce           int
}




func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Try map tasks first
	if c.CompletedMaps < len(c.MapInputFiles) {
		if task, ok := c.assignTask(Map, reply); ok {
			reply.File = c.MapInputFiles[task]
			reply.NReduce = c.nReduce
			return nil
		}
		// Maps still in progress, tell worker to wait
		reply.TaskType = Wait
		return nil
	}

	// All maps done, try reduce tasks
	if _, ok := c.assignTask(Reduce, reply); ok {
		reply.NMap = len(c.MapInputFiles)
		return nil
	}

	// Reduces still in progress, tell worker to wait

	if c.CompletedReduces < c.nReduce {
		reply.TaskType = Wait
	} else {
		reply.TaskType = Done
	}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == Map {
		c.MapTasksStatus[args.TaskID].status = Completed
		c.CompletedMaps += 1
	} else if args.TaskType == Reduce {
		c.ReduceTasksStatus[args.TaskID].status = Completed
		c.CompletedReduces += 1
	}

	// fmt.Printf("task %d finished\n", args.TaskID)
	return nil
}


func (c *Coordinator) CheckStalledTask(t TaskStatus) bool {
	if t.status == InProgress && time.Since(t.startTime) > 10*time.Second {
			return true
	}
	return false

}

func (c* Coordinator) GetNextTask(tasks []TaskStatus) (int, bool) {
	for i, t := range tasks {
		if t.status == NotStarted {
			return i, false
		}else if isStalled := c.CheckStalledTask(t); isStalled {
			return i, false
		}
	}
	return -1, true
}


func (c *Coordinator) assignTask(taskType Task, reply *RequestTaskReply) (int, bool) {
	tasks := c.getTasksForType(taskType)


	// using GetNextTask to find tasks that are stalled alongside
	// tasks that are not started yet
	// rather than waiting for all the didn't start tasks to be finished
	taskID, completed := c.GetNextTask(tasks)

	if completed {
		return -1, false // No tasks available, should wait
	}

	// reassign a stalled task
	c.markTaskInProgress(taskType, taskID)
	reply.TaskID = taskID
	reply.TaskType = taskType
	return taskID, true
}

func (c *Coordinator) getTasksForType(taskType Task) []TaskStatus {
	if taskType == Map {
		return c.MapTasksStatus
	}
	return c.ReduceTasksStatus
}

func (c *Coordinator) markTaskInProgress(taskType Task, taskID int) {
	if taskType == Map {
		c.MapTasksStatus[taskID].status = InProgress
		c.MapTasksStatus[taskID].startTime = time.Now()
	} else if taskType == Reduce {
		c.ReduceTasksStatus[taskID].status = InProgress
		c.ReduceTasksStatus[taskID].startTime = time.Now()
	}
}

// func (c *Coordinator) findStalledTask(taskType Task) (int, error) {
// 	tasks := c.getTasksForType(taskType)

// 	for i, t := range tasks {
// 		if t.status == InProgress && time.Since(t.startTime) > 10*time.Second {
// 			return i, nil
// 		}
// 	}
// 	return -1, fmt.Errorf("no stuck tasks")
// }

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CompletedReduces == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapInputFiles = files
	c.nReduce = nReduce
	c.MapTasksStatus = make([]TaskStatus, len(files))
	c.ReduceTasksStatus = make([]TaskStatus, nReduce)
	c.CompletedMaps = 0
	c.CompletedReduces = 0
	c.server(sockname)
	return &c
}
