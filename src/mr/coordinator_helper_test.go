package mr

import (
	"testing"
	"time"
)

func Test_init(t *testing.T) {
	c := &Coordinator{}
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

	c.init(files, 10)
	c.server()

	time.Sleep(time.Minute * 5)
}
