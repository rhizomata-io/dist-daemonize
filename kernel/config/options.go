package config

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

// RunOptions : options for running kernel
type RunOptions struct {
	Cluster  string
	Name     string
	Hostname string
	Port     uint
	DataDir  string
	EtcdUrls []string
	// HeartbeatInterval Heartbeat Interval
	HeartbeatInterval uint

	// CheckHeartbeatInterval Heartbeat check Interval
	CheckHeartbeatInterval uint

	// AliveThreasholdSecond Heartbeat time Threashold
	AliveThreasholdSeconds uint
}

//NewRunOptions make new default RunOptions
func NewRunOptions() (runOptions *RunOptions) {
	runOptions = new(RunOptions)

	runOptions.Cluster = "default"
	runOptions.Name = "dd1"
	runOptions.Hostname = "0.0.0.0"
	runOptions.Port = 12791
	runOptions.DataDir = "daemon-data/" + runOptions.Name
	runOptions.EtcdUrls = []string{"http://127.0.0.1:2379"}
	runOptions.HeartbeatInterval = 2 * uint(time.Second)
	runOptions.CheckHeartbeatInterval = 3 * uint(time.Second)
	runOptions.AliveThreasholdSeconds = 5

	return runOptions
}

// ParseRunOptions : parses flags
func ParseRunOptions() (runOptions *RunOptions) {
	clusterName := flag.String("cluster", "cluster1", "name of cluster")
	name := flag.String("name", "bridge1", "name of etcd server")
	host := flag.String("exposed-host", "0.0.0.0", "host name/IP")
	port := flag.Uint("port", 12791, "liesten port for daemon")
	dataDir := flag.String("data-dir", "daemon-data", "local data directory")
	etcdUrls := flag.String("etcd-urls", "", "etcd-urls,...")
	heartbeatInterval := flag.Uint("heartbeat-interval", 2, "heartbeat interval(seconds)")
	checkHeartbeatInterval := flag.Uint("heartbeat-check-interval", 3, "heartbeat check interval(seconds)")
	aliveThreasholdSeconds := flag.Uint("alive-threashold", 5, "alive threashold seconds")

	flag.Parse()

	runOptions = new(RunOptions)
	runOptions.Cluster = *clusterName
	runOptions.Name = *name
	runOptions.Hostname = *host
	runOptions.Port = *port
	runOptions.DataDir = *dataDir + "/" + *name

	if !strings.Contains(",", *etcdUrls) {
		runOptions.EtcdUrls = []string{*etcdUrls}
	} else {
		runOptions.EtcdUrls = strings.Split(",", *etcdUrls)
	}

	runOptions.HeartbeatInterval = *heartbeatInterval * uint(time.Second)
	runOptions.CheckHeartbeatInterval = *checkHeartbeatInterval * uint(time.Second)
	runOptions.AliveThreasholdSeconds = *aliveThreasholdSeconds

	return runOptions
}

// GetServiceAddr ..
func (runOptions RunOptions) GetServiceAddr() string {
	return runOptions.Hostname + ":" + fmt.Sprint(runOptions.Port)
}

// GetServiceURL http://{GetServiceAddr}
func (runOptions RunOptions) GetServiceURL() string {
	return "http://" + runOptions.GetServiceAddr()
}
