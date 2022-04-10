package mr

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
)

func Test_genMapOutputFilename(t *testing.T) {
	for m := 0; m < 8; m++ {
		for r := 0; r < 10; r++ {
			fmt.Println(genMapOutputFilename(m, r))
		}
	}
}

func Test_doReduce(t *testing.T) {
	start := time.Now()
	defer func() {
		fmt.Println(time.Now().Sub(start))
	}()

	tasks := &TaskResponse{}
	tasks.TotalMaps = 8
	task := &Task{}
	tasks.Tasks = make([]*Task, 0)
	tasks.Tasks = append(tasks.Tasks, task)
	for i := 0; i < 10; i++ {
		task.TaskId = i
		doReduce(tasks, func(key string, values []string) string {
			// return the number of occurrences of this word.
			return strconv.Itoa(len(values))
		})
	}
}

func Test_doMap(t *testing.T) {
	start := time.Now()
	defer func() {
		fmt.Println(time.Now().Sub(start))
	}()

	files := []string{
		"../main/pg-being_ernest.txt",
		"../main/pg-dorian_gray.txt",
		"../main/pg-frankenstein.txt",
		"../main/pg-grimm.txt",
		"../main/pg-huckleberry_finn.txt",
		"../main/pg-metamorphosis.txt",
		"../main/pg-sherlock_holmes.txt",
		"../main/pg-tom_sawyer.txt",
	}

	tasks := &TaskResponse{}
	tasks.Tasks = make([]*Task, 0)
	tasks.TotalReduces = 10

	id := 0
	for _, file := range files {
		tasks.Tasks = append(tasks.Tasks, &Task{TaskId: id, File: file})
		id++
	}

	doMap(tasks, func(filename string, contents string) []KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	})
}
