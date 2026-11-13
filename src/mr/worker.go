package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return
		}
		switch reply.Type {
		case MapTask:
			doMap(reply, mapf)
			doneArgs := ReportDoneArgs{Task{Type: MapTask, ID: reply.ID}}
			doneReply := ReportDoneReply{}
			call("Coordinator.ReportDone", &doneArgs, &doneReply)
		case ReduceTask:
			doReduce(reply, reducef)
			doneArgs := ReportDoneArgs{Task{Type: ReduceTask, ID: reply.ID}}
			doneReply := ReportDoneReply{}
			call("Coordinator.ReportDone", &doneArgs, &doneReply)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(reply GetTaskReply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(reply.File)
	if err != nil {
		log.Fatalf("doMap: cannot read %v: %v", reply.File, err)
	}
	kva := mapf(reply.File, string(content))
	nReduce := reply.N
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		buckets[r] = append(buckets[r], kv)
	}
	for r := 0; r < nReduce; r++ {
		tmp, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("doMap: CreateTemp: %v", err)
		}
		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("doMap: encode: %v", err)
			}
		}
		tmp.Close()

		outName := fmt.Sprintf("mr-%d-%d", reply.ID, r)
		if err := os.Rename(tmp.Name(), outName); err != nil {
			log.Fatalf("doMap: rename to %v: %v", outName, err)
		}
	}
}

func doReduce(reply GetTaskReply, reducef func(string, []string) string) {
	nMap := reply.N
	intermediate := make(map[string][]string)
	for m := 0; m < nMap; m++ {
		fname := fmt.Sprintf("mr-%d-%d", m, reply.ID)
		f, err := os.Open(fname)
		if err != nil {
			log.Fatalf("doReduce: open %v: %v", fname, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("doReduce: decode %v: %v", fname, err)
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
		f.Close()
	}
	keys := make([]string, 0, len(intermediate))
	for k := range intermediate {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tmp, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("doReduce: CreateTemp: %v", err)
	}
	for _, k := range keys {
		v := reducef(k, intermediate[k])
		fmt.Fprintf(tmp, "%v %v\n", k, v)
	}
	tmp.Close()
	outName := fmt.Sprintf("mr-out-%d", reply.ID)
	if err := os.Rename(tmp.Name(), outName); err != nil {
		log.Fatalf("doReduce: rename to %v: %v", outName, err)
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
