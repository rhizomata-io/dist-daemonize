# Distributed Daemonize

### Distributed Daemon kernel based on etcd

```
go get github.com/rhizomata-io/dist-daemonize/...
```


``` golang
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
		daemonizer.StartDiscovery()


		job1 := job.NewWithPIAndID("job1", "sample", `{"greet":"hello"}`)
		daemonizer.AddJobIfNotExists(job1)

		job2 := job.NewWithPIAndID("job2", "sample", `{"greet":"hi"}`)
		daemonizer.AddJobIfNotExists(job2)

		job3 := job.NewWithPIAndID("job3", "sample", `{"greet":"What's up"}`)
		daemonizer.AddJobIfNotExists(job3)


		daemonizer.Wait()
	}
}
```

```bash
go run . -cluster cluster1 -name dd1 -exposed-host 127.0.0.1 -port 12345 -etcd-urls http://127.0.0.1:2379

go run . -cluster cluster1 -name dd2 -exposed-host 127.0.0.1 -port 12346 -etcd-urls http://127.0.0.1:2379
```

* [Simple Sample](http://github.com/rhizomata-io/dist-daemonize/tree/master/samples/simple)
* [Discovery Sample](http://github.com/rhizomata-io/dist-daemonize/tree/master/samples/discovery)