package main

import (
	"github.com/rhizomata-io/dist-daemonize/dd"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
)

func main() {
	runOptions := config.ParseRunOptions()

	if daemonizer, err := dd.Daemonize(runOptions); err == nil {
		factory := new(SampleFactory)
		daemonizer.RegisterWorkerFactory(factory)
		daemonizer.Start()

		job1 := job.NewWithPIAndID("job1", "sample", `{"greet":"hello"}`)
		daemonizer.AddJobIfNotExists(job1)

		job2 := job.NewWithPIAndID("job2", "sample", `{"greet":"hi"}`)
		daemonizer.AddJobIfNotExists(job2)

		job3 := job.NewWithPIAndID("job3", "sample", `{"greet":"What's up"}`)
		daemonizer.AddJobIfNotExists(job3)

		daemonizer.Wait()
	}
}
