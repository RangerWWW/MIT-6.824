package mr

import (
	"sort"
	"sync"
)

const (
	mapTask    = int8(iota + 1)
	reduceTask = mapTask + 1
)

type TaskRequest struct {
	TaskType int8
	WorkerId int
}

type TaskResponse struct {
	TaskType     int8
	WorkerId     int
	TotalMaps    int
	TotalReduces int
	Tasks        []*Task
	Retry        bool
	Done         bool
}

type Task struct {
	TaskId   int
	WorkId   int
	File     string
	ReduceId int
	Done     bool
}

type tasks struct {
	data []*Task
	done bool
	lock sync.Mutex
}

func (r *tasks) isDone() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.done
}

func (r *tasks) nextTask() *Task {
	if r.isDone() {
		return nil
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	var ret *Task
	for _, item := range r.data {
		if !item.Done && item.WorkId == 0 {
			ret = item
			break
		}
	}
	return ret
}

func (r *tasks) isTaskDone(taskId int) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	ret := true
	if taskId < len(r.data) {
		ret = r.data[taskId].Done
	}
	return ret
}

func (r *tasks) resetWorker(taskId int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if taskId < len(r.data) &&
		!r.data[taskId].Done {
		r.data[taskId].WorkId = 0
	}
}

func (r *tasks) taskDone(taskId int, workId int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if taskId < len(r.data) &&
		r.data[taskId].WorkId == workId {
		r.data[taskId].Done = true
	}
	for _, item := range r.data {
		if !item.Done {
			return
		}
	}
	r.done = true
}

func newMapTasks(files []string) *tasks {
	sort.Strings(files)
	ret := &tasks{}
	for i, file := range files {
		ret.data = append(ret.data, &Task{TaskId: i, File: file})
	}
	return ret
}

func newReduceTasks(nReduce int) *tasks {
	ret := &tasks{}
	for i := 0; i < nReduce; i++ {
		ret.data = append(ret.data, &Task{TaskId: i, ReduceId: i})
	}
	return ret
}

type TaskStatus struct {
	TaskType int8
	WorkerId int
	TaskIds  []int
	Done     bool
	Err      error
}
