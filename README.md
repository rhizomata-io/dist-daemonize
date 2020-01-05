# Distributed Daemonize

### Distributed Daemon kernel based on etcd

```
go get github.com/rhizomata-io/dist-daemonize/...
```


```
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
		daemonizer.Wait()
	}
}
```


#1 [Simple Sample](tree/master/samples/simple)
#2 [Discovery Sample](tree/master/samples/discovery)