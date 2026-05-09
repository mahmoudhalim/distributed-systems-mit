package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname
	args := RequestTaskArgs{WorkerID: os.Getpid()}

	for {
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			// fmt.Printf("call failed!\n")
			return
		}

		switch reply.TaskType {
		case Map:
			// fmt.Printf("map task %d Starting ...\n", reply.TaskID)
			err := startMapTask(mapf, reply.File, reply.NReduce, reply.TaskID)
			if err != nil {
				panic(err)
			}
		case Reduce:
			// fmt.Printf("reduce task %d \n", reply.TaskID)
			err := startReduceTask(reducef, reply.TaskID, reply.NMap)
			if err != nil {
				panic(err)
			}
		case Wait:
			time.Sleep(time.Second)
			// fmt.Printf("worker is waiting\n")
		case Done:
			fmt.Println("Task Done!")
			os.Exit(0)
		default:
			panic("Bad Task")
		}

		// fmt.Printf("task %d Finished\n", reply.TaskID)
	}
}

func startMapTask(mapf func(string, string) []KeyValue, filename string, nReduce int, taskID int) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	mapRes := mapf(filename, string(content))
	sort.Sort(ByKey(mapRes))

	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", taskID, i)
		ofile, err := os.CreateTemp(".", fmt.Sprintf("%s-*", oname))
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		files[i] = ofile
		encoders[i] = json.NewEncoder(ofile)
	}

	for _, kv := range mapRes {
		reduceTarget := ihash(kv.Key) % nReduce
		err := encoders[reduceTarget].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	for _, f := range files {
		f.Close()
		newName := f.Name()[:strings.LastIndex(f.Name(), "-")]
		os.Rename(f.Name(), newName)
	}
	args := ReportTaskArgs{TaskID: taskID}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
	return nil
}

func startReduceTask(reducef func(string, []string) string, taskID int, nMap int) error {
	dir, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("cannot read dir %v", err)
	}

	files := make([]string, 0, nMap)
	for _, f := range dir {
		if strings.HasSuffix(f.Name(), fmt.Sprintf("-%d", taskID)) {
			files = append(files, f.Name())
		}
	}
	// PERF: consider pre allocating the kv slice
	// resolved 
	kvs := make([]KeyValue, 0, 1000)
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			log.Fatalf("cannot open file %v", f)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	sort.Sort(ByKey(kvs))

	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-*", taskID))
	if err != nil {
		log.Fatalf("cannot create temp file %v", err)
	}

	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		res := reducef(kvs[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, res)

		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", taskID))
	call("Coordinator.ReportTask", &ReportTaskArgs{TaskID: taskID, TaskType: Reduce}, &ReportTaskReply{})

	return nil
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
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
