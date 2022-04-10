package mr

import (
	"io"
	"log"
	"time"
)

const taskTimeoutDuration = time.Second * 10

func (c *Coordinator) init(files []string, nReduce int) {
	log.SetOutput(io.Discard)

	c.workers = 3
	c.nReduce = nReduce
	c.nMap = len(files)
	c.inputs = append(c.inputs, files...)
	c.mapTasks = newMapTasks(files)
	c.reduceTasks = newReduceTasks(nReduce)
}

func (c *Coordinator) RequestTask(req *TaskRequest, reply *TaskResponse) error {
	task := c.mapTasks.nextTask()
	if task != nil {
		req.TaskType = mapTask
		c.fillReply(req, reply, task, false, false)
		go taskTimeout(c.mapTasks, task, taskTimeoutDuration)
		goto exit
	}

	if !c.mapTasks.isDone() {
		c.fillReply(req, reply, nil, true, false)
		goto exit
	}

	task = c.reduceTasks.nextTask()
	if task != nil {
		req.TaskType = reduceTask
		c.fillReply(req, reply, task, false, false)
		go taskTimeout(c.reduceTasks, task, taskTimeoutDuration)
		goto exit
	}

	if !c.reduceTasks.isDone() {
		c.fillReply(req, reply, nil, true, false)
		goto exit
	} else {
		c.fillReply(req, reply, nil, false, true)
		goto exit
	}

exit:
	log.Printf("accept request %v and reply %v\n", req, reply)
	return nil
}

func (c *Coordinator) fillReply(req *TaskRequest, reply *TaskResponse, task *Task, retry bool, done bool) {
	reply.TaskType = req.TaskType
	reply.WorkerId = req.WorkerId
	if task != nil {
		task.WorkId = req.WorkerId
		tmp := &Task{
			TaskId:   task.TaskId,
			WorkId:   task.WorkId,
			File:     task.File,
			ReduceId: task.ReduceId,
			Done:     task.Done,
		}
		reply.Tasks = append(reply.Tasks, tmp)
	}
	reply.TotalReduces = c.nReduce
	reply.TotalMaps = c.nMap
	reply.Retry = retry
	reply.Done = done
}

func (c *Coordinator) TaskStatus(req *TaskStatus, reply *bool) error {
	*reply = true
	if mapTask == req.TaskType {
		for _, id := range req.TaskIds {
			c.mapTasks.taskDone(id, req.WorkerId)
			log.Printf("map task %d is done by worker %d\n", id, req.WorkerId)
		}
	} else if reduceTask == req.TaskType {
		for _, id := range req.TaskIds {
			c.reduceTasks.taskDone(id, req.WorkerId)
			log.Printf("reduce task %d is done by worker %d\n", id, req.WorkerId)
		}
	}
	return nil
}

func taskTimeout(tasks *tasks, task *Task, timeout time.Duration) {
	time.AfterFunc(timeout, func() {
		tasks.resetWorker(task.TaskId)
	})
}
