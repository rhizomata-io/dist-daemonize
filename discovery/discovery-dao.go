package discovery

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
)

const (
	kvDirSys      = "/$sys/"
	kvDirClusters = kvDirSys + "clstrs/"
	kvDiscovery   = kvDirClusters + "%s/discovery"
)

// DAO Discovery DAO
type DAO struct {
	cluster string
	kv      kv.KV
}

// GetDiscoveryInfo ..
func (dao *DAO) GetDiscoveryInfo() (membJobMap map[string][]string, err error) {
	membJobMap = make(map[string][]string)
	err = dao.kv.GetObject(fmt.Sprintf(kvDiscovery, dao.cluster), &membJobMap)
	return membJobMap, err
}

// PutDiscoveryInfo ..
func (dao *DAO) PutDiscoveryInfo(membJobMap map[string][]string) (err error) {
	_, err = dao.kv.PutObject(fmt.Sprintf(kvDiscovery, dao.cluster), membJobMap)
	return err
}

// WatchDiscoveryInfo ..
func (dao *DAO) WatchDiscoveryInfo(handler func(membJobMap map[string][]string)) (watcher *kv.Watcher) {
	watcher = dao.kv.Watch(fmt.Sprintf(kvDiscovery, dao.cluster),
		func(key string, value []byte) {
			membJobMap := make(map[string][]string)
			err := json.Unmarshal(value, &membJobMap)
			if err != nil {
				log.Println("[ERROR-DiscoveryWatch] ", err)
			} else {
				handler(membJobMap)
			}
		})
	return watcher
}
