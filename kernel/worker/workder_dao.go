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
	fullPath := fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID)
	_, err := dao.kv.Put(fullPath, data)
	if err != nil {
		log.Println("[ERROR-WorkerDao] PutData", err)
	}
	// fmt.Println("&&&&& PutData :: fullPath=", fullPath, ", data=", data)
	return err
}

// PutDataFullPath ..
func (dao *DAO) PutDataFullPath(fullPath string, data string) error {
	_, err := dao.kv.Put(fullPath, data)
	if err != nil {
		log.Println("[ERROR-WorkerDao] PutDataFullPath", err)
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
	// fmt.Println("&&&&& PutObject :: key=", key, ", data=", data)
	return err
}

// PutObjectFullPath ..
func (dao *DAO) PutObjectFullPath(fullPath string, data interface{}) error {
	_, err := dao.kv.PutObject(fullPath, data)
	if err != nil {
		log.Println("[ERROR-WorkerDao] PutObjectFullPath", err)
	}
	// fmt.Println("&&&&& PutObjectFullPath :: fullPath=", fullPath, ", data=", data)
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
	key := fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID)
	_, err := dao.kv.DeleteOne(key)
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetData ", err)
	}
	// fmt.Println("^^^^^^ DeleteData :: key=", key)
	return err
}

// DeleteDataFullPath ..
func (dao *DAO) DeleteDataFullPath(key string) error {
	_, err := dao.kv.DeleteOne(key)
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetData ", err)
	}
	// fmt.Println("^^^^^^ DeleteDataFullPath :: key=", key)
	return err
}

// GetDataWithTopic ..
func (dao *DAO) GetDataWithTopic(jobid string, topic string, handler kv.DataHandler) error {
	err := dao.kv.GetWithPrefix(fmt.Sprintf(kvPatternDataTopic, dao.cluster, jobid, topic), handler)
	if err != nil {
		log.Println("[ERROR-WorkerDao] GetDataWithJobID ", err)
	}
	return err
}

// WatchDataWithTopic ..
func (dao *DAO) WatchDataWithTopic(jobid string, topic string,
	handler func(eventType kv.EventType, fullPath string, rowID string, value []byte)) *kv.Watcher {
	key := fmt.Sprintf(kvPatternDataTopic, dao.cluster, jobid, topic)
	watcher := dao.kv.WatchWithPrefix(key, handler)
	// fmt.Println("&&&&& WatchDataWithTopic :: key=", key)

	return watcher
}

// WatchData ..
func (dao *DAO) WatchData(jobid string, topic string, rowID string,
	handler func(eventType kv.EventType, fullPath string, rowID string, value []byte)) *kv.Watcher {
	key := fmt.Sprintf(kvPatternData, dao.cluster, jobid, topic, rowID)
	watcher := dao.kv.Watch(key, handler)
	// fmt.Println("&&&&& WatchData :: key=", key)

	return watcher
}
