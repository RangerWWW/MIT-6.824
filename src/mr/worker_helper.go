package mr

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func workInternal(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.SetOutput(io.Discard)

	for {
		task, err := RequestTasks()
		if err != nil {
			log.Fatalf("failed to request task %v\n", err)
		}

		if task.Done {
			break
		}

		if mapTask == task.TaskType {
			doMap(task, mapf)
			reportSuccess(task)
		} else if reduceTask == task.TaskType {
			doReduce(task, reducef)
			reportSuccess(task)
		}

		if task.Retry {
			time.Sleep(time.Millisecond * 100)
			continue
		}
	}

	log.Printf("worker %d end\n", os.Getpid())
}

func reportSuccess(task *TaskResponse) {
	ids := make([]int, 0, len(task.Tasks))
	for _, item := range task.Tasks {
		ids = append(ids, item.TaskId)
	}
	ReportStatus(&TaskStatus{
		TaskType: task.TaskType,
		WorkerId: task.WorkerId,
		TaskIds:  ids,
		Done:     true,
	})
}

// reduce

func doReduce(tasks *TaskResponse, reducef func(string, []string) string) {
	var wg sync.WaitGroup
	for _, task := range tasks.Tasks {
		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			log.Printf("start process reduce task %d at %v\n", task.TaskId, time.Now())
			// read and sort
			kvs := collectMapOutput(task.TaskId, tasks.TotalMaps)
			// reduce and save
			callReduce(task.TaskId, kvs, reducef)
		}(task)
	}
	wg.Wait()
}

func callReduce(reduceTaskId int, intermediate []KeyValue, reducef func(string, []string) string) {
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%d", reduceTaskId))
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func collectMapOutput(reduceTaskId int, mapTasks int) []KeyValue {
	var ret = make([]KeyValue, 0, 50)
	for mapTaskId := 0; mapTaskId < mapTasks; mapTaskId++ {
		ret = append(ret, parseOneMapOutput(mapTaskId, reduceTaskId)...)
	}
	sort.Sort(ByKey(ret))
	return ret
}

func parseOneMapOutput(mapId, reduceId int) []KeyValue {
	var ret = make([]KeyValue, 0, 50)
	filename := genMapOutputFilename(mapId, reduceId)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	text := string(content)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		columns := strings.Split(line, ",")
		if len(columns) != 2 {
			log.Printf("failed to parse line %s in file %s\n", line, filename)
			continue
		}
		ret = append(ret, KeyValue{
			Key:   columns[0],
			Value: columns[1],
		})
	}
	return ret
}

// map

func doMap(tasks *TaskResponse, mapf func(string, string) []KeyValue) {
	var wg sync.WaitGroup
	for _, task := range tasks.Tasks {
		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			log.Printf("start process map task %d at %v\n", task.TaskId, time.Now())
			kvs := callMap(task, mapf)
			saveMap(tasks.TotalReduces, task.TaskId, kvs)
		}(task)
	}
	wg.Wait()
}

func saveMap(reduceTasks, mapTaskId int, kvs []KeyValue) {
	for k, v := range sliceToMap(kvs, reduceTasks) {
		filename := genMapOutputFilename(mapTaskId, k)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("failed to create file %s: %v\n", filename, err)
			return
		}
		defer file.Close()
		for _, kv := range v {
			fmt.Fprintf(file, "%s,%s\n", kv.Key, kv.Value)
		}
	}
}

func callMap(task *Task, mapf func(string, string) []KeyValue) []KeyValue {
	filename := task.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return mapf(filename, string(content))
}

func sliceToMap(s []KeyValue, num int) map[int][]KeyValue {
	ret := make(map[int][]KeyValue, 10)
	for _, item := range s {
		reduceId := ihash(item.Key) % num
		value := ret[reduceId]
		if value == nil {
			value = make([]KeyValue, 0)
		}
		value = append(value, item)
		ret[reduceId] = value
	}
	return ret
}

func genMapOutputFilename(mapTaskId, reduceTaskId int) string {
	return fmt.Sprintf("mr-%02d-%02d", mapTaskId, reduceTaskId)
}

func RequestTasks() (*TaskResponse, error) {
	req := &TaskRequest{
		WorkerId: os.Getpid(),
	}

	resp := &TaskResponse{}
	resp.Tasks = make([]*Task, 0, 5)

	b := call("Coordinator.RequestTask", req, resp)
	log.Printf("Request task %v and reply %v\n", req, resp)
	if b {
		return resp, nil
	} else {
		return nil, fmt.Errorf("failed to request map tasks")
	}
}

func ReportStatus(status *TaskStatus) bool {
	var reply bool
	log.Printf("Report task status %v\n", status)
	return call("Coordinator.TaskStatus", status, &reply)
}
