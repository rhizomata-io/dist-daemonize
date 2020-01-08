package config

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
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

const (
	_cluster                = "$DD-CLUSTER"
	_name                   = "$DD-CLUSTER"
	_exposedHost            = "$DD-EXPOSED-HOST"
	_port                   = "$DD-PORT"
	_dataDir                = "$DD-DATADIR"
	_etcdUrls               = "$DD-ETCD-URLS"
	_heartbeatInterval      = "$DD-HEARTBEAT-INT"
	_checkHeartbeatInterval = "$DD-CHECK-HEARTBEAT-INT"
	_aliveThreasholdSeconds = "$DD-ALIVE-THRESHOLD"
)

func parseString(envName string, argName string, usage string, defVal string) (valueRef *string) {
	value := os.Getenv(envName)
	if len(value) == 0 {
		value = defVal
	}
	valueRef = flag.String(argName, value, usage)
	return valueRef
}

func parseUint(envName string, argName string, usage string, defVal uint) (valueRef *uint) {
	value := os.Getenv(envName)
	var uintVal uint
	if len(value) == 0 {
		uintVal = defVal
	} else {
		val, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			log.Fatal("Cannot Parse env ", envName, "<-", value)
		}
		uintVal = uint(val)
	}
	valueRef = flag.Uint(argName, uintVal, usage)
	return valueRef
}

// ParseRunOptions : parses flags
func ParseRunOptions() (runOptions *RunOptions) {
	clusterName := parseString(_cluster, "cluster", "name of cluster", "default")
	name := parseString(_name, "name", "name of etcd server", "dd1")
	host := parseString(_exposedHost, "exposed-host", "host name/IP", "0.0.0.0")
	port := parseUint(_port, "port", "liesten port for daemon", 12791)
	dataDir := parseString(_dataDir, "data-dir", "local data directory", "daemon-data")
	etcdUrls := parseString(_etcdUrls, "etcd-urls", "etcd-urls,...", "http://127.0.0.1:2379")
	heartbeatInterval := parseUint(_heartbeatInterval, "heartbeat-interval", "heartbeat interval(seconds)", 2)
	checkHeartbeatInterval := parseUint(_checkHeartbeatInterval, "heartbeat-check-interval", "heartbeat check interval(seconds)", 3)
	aliveThreasholdSeconds := parseUint(_aliveThreasholdSeconds, "alive-threashold", "alive threashold seconds", 5)

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
