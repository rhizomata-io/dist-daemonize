package main

import (
	"fmt"
	"log"
	"time"

	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
)

// SimpleFactory implements worker.Factory
type SimpleFactory struct {
}

// Name implements worker.Factory.Name as 'sample'
func (factory *SimpleFactory) Name() string { return "sample" }

// NewWorker implements worker.Factory.NewWorker
func (factory *SimpleFactory) NewWorker(helper *worker.Helper) (worker worker.Worker, err error) {
	jobInfo := &JobInfo{}
	helper.Job().GetAsObject(jobInfo)

	log.Println("helper.ID()::", helper.ID())
	log.Println("JOB::", helper.Job())
	log.Println("jobInfo::", jobInfo)
	log.Println("job Data::", string(helper.Job().Data))

	worker = &SimpleWorker{id: helper.ID(), helper: helper, jobInfo: jobInfo}

	return worker, err
}

// SimpleWorker implements worker.Worker
type SimpleWorker struct {
	id      string
	helper  *worker.Helper
	jobInfo *JobInfo
	started bool
}

// JobInfo job info object
type JobInfo struct {
	Greet string `json:"greet"`
}

// CheckPoint CheckPoint
type CheckPoint struct {
	Count int64 `json:"count"`
}

//ID ..
func (worker *SimpleWorker) ID() string {
	return worker.id
}

//Start ..
func (worker *SimpleWorker) Start() error {
	worker.started = true
	log.Printf("Simple Worker [%s] Started.\n", worker.ID())
	for worker.started {
		var checkpoint CheckPoint
		worker.helper.GetCheckpoint(&checkpoint)

		fmt.Printf("[%s] %s : %d\n", worker.ID(), worker.jobInfo.Greet, checkpoint.Count)
		checkpoint.Count = checkpoint.Count + 1

		worker.helper.PutCheckpoint(checkpoint)

		time.Sleep(time.Second * 1)
	}
	return nil
}

//Stop ..
func (worker *SimpleWorker) Stop() error {
	worker.started = false
	log.Printf("Simple Worker [%s] Stopped.\n", worker.ID())
	return nil
}

//IsStarted ..
func (worker *SimpleWorker) IsStarted() bool {
	return worker.started
}
