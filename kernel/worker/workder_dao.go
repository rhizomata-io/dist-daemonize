package worker

import (
	"fmt"
	"log"

	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
)

const (
	kvDirSys            = "/$sys/"
	kvDirClusters       = kvDirSys + "clstrs/"
	kvPatternCheckpoint = kvDirClusters + "%s/checkpoint/%s"
	kvPatternDataJobID  = kvDirClusters + "%s/data/%s/"
	kvPatternDataTopic  = kvPatternDataJobID + "%s/"
	kvPatternData       = kvPatternDataTopic + "%s"
)

// DAO kv store model for cluster
type DAO struct {
	cluster string
	kv      kv.KV
}

// PutCheckpoint ..
func (dao *DAO) PutCheckpoint(jobid string, checkpoint interface{}) error {
	_, err := dao.kv.PutObject(fmt.Sprintf(kvPatternCheckpoint, dao.cluster, jobid), checkpoint)
	if err != nil {
		log.Println("[ERROR-WorkerDao] PutCheckpoint", err)
	}
	return err
}

// GetCheckpoint ..
func (dao *DAO) GetCheckpoint(jobid string, checkpoint interface{}) error {
	err := dao.kv.GetObject(fmt.Sprintf(kvPatternCheckpoint, dao.cluster, jobid), checkpoint)
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetCheckpoint ", err)
	}
	return err
}

// PutData ..
func (dao *DAO) PutData(jobid string, topic string, rowID string, data string) error {
	_, err := dao.kv.Put(fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID), data)
	if err != nil {
		log.Println("[ERROR-WorkerDao] PutData", err)
	}
	return err
}

// PutObject ..
func (dao *DAO) PutObject(jobid string, topic string, rowID string, data interface{}) error {
	key := fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID)
	_, err := dao.kv.PutObject(key, data)
	if err != nil {
		log.Println("[ERROR-WorkerDao] PutObject", err)
	}
	fmt.Println("&&&&& PutObject :: key=", key, ", data=", data)
	return err
}

// GetData ..
func (dao *DAO) GetData(jobid string, topic string, rowID string) (data []byte, err error) {
	data, err = dao.kv.GetOne(fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID))
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetData ", err)
	}
	return data, err
}

// GetObject ..
func (dao *DAO) GetObject(jobid string, topic string, rowID string, data interface{}) error {
	err := dao.kv.GetObject(fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID), data)
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetObject ", err)
	}
	return err
}

// DeleteData ..
func (dao *DAO) DeleteData(jobid string, topic string, rowID string) error {
	_, err := dao.kv.DeleteOne(fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID))
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetData ", err)
	}
	return err
}

// GetDataWithTopic ..
func (dao *DAO) GetDataWithTopic(jobid string, topic string, handler func(key string, value []byte)) error {
	err := dao.kv.GetWithPrefix(fmt.Sprintf(kvPatternDataTopic, dao.cluster, jobid, topic), handler)
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetDataWithJobID ", err)
	}
	return err
}

// WatchDataWithTopic ..
func (dao *DAO) WatchDataWithTopic(jobid string, topic string, handler func(key string, value []byte)) *kv.Watcher {
	key := fmt.Sprintf(kvPatternDataTopic, dao.cluster, jobid, topic)
	watcher := dao.kv.WatchWithPrefix(key, handler)
	fmt.Println("&&&&& WatchDataWithTopic :: key=", key)

	return watcher
}
