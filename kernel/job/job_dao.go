package job

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
)

const (
	kvDirSys           = "/$sys/"
	kvDirClusters      = kvDirSys + "clstrs/"
	kvDirMemberJob     = kvDirClusters + "%s/membjob/"
	kvPatternMemberJob = kvDirMemberJob + "%s"
	kvPatternJobsDir   = kvDirClusters + "%s/jobs/"
	kvPatternJob       = kvPatternJobsDir + "%s"
)

// DAO kv store model for job
type DAO struct {
	cluster string
	kv      kv.KV
}

// GetMemberJobs ..
func (dao *DAO) GetMemberJobs(membID string) (jobIDs []string, err error) {
	jobIDs = []string{}
	err = dao.kv.GetObject(fmt.Sprintf(kvPatternMemberJob, dao.cluster, membID), jobIDs)
	return jobIDs, err
}

// GetAllMemberJobIDs : returns member-JobIDs Map
func (dao *DAO) GetAllMemberJobIDs() (membJobMap map[string][]string, err error) {
	membJobMap = make(map[string][]string)
	dirPath := fmt.Sprintf(kvDirMemberJob, dao.cluster)
	err = dao.kv.GetWithPrefix(dirPath,
		func(fullPath string, rowID string, value []byte) bool {
			jobIDs := []string{}
			err := json.Unmarshal(value, &jobIDs)
			if err != nil {
				log.Println("[ERROR-JobDao] unmarshal member jobs ", fullPath, err)
			}
			membid := rowID
			membJobMap[membid] = jobIDs
			return true
		})

	return membJobMap, err
}

// PutMemberJobs ..
func (dao *DAO) PutMemberJobs(membID string, jobIDs []string) (err error) {
	_, err = dao.kv.PutObject(fmt.Sprintf(kvPatternMemberJob, dao.cluster, membID), jobIDs)
	return err
}

// WatchMemberJobs ..
func (dao *DAO) WatchMemberJobs(memberID string, handler func(jobIDs []string)) (watcher *kv.Watcher) {
	dirPath := fmt.Sprintf(kvPatternMemberJob, dao.cluster, memberID)
	watcher = dao.kv.Watch(dirPath,
		func(eventType kv.EventType, fullPath string, rowID string, value []byte) {
			jobIDs := []string{}
			err := json.Unmarshal(value, &jobIDs)
			if err != nil {
				log.Println("[ERROR-JobDao] unmarshal member jobs ", memberID, err)
			}
			handler(jobIDs)
		})
	return watcher
}

// GetJob ..
func (dao *DAO) GetJob(jobID string) (job Job, err error) {
	value, err := dao.kv.GetOne(fmt.Sprintf(kvPatternJob, dao.cluster, jobID))
	return Job{ID: jobID, Data: value}, err
}

// ContainsJob ..
func (dao *DAO) ContainsJob(jobID string) bool {
	value, err := dao.kv.GetOne(fmt.Sprintf(kvPatternJob, dao.cluster, jobID))
	return err == nil && value != nil
}

// PutJob ..
func (dao *DAO) PutJob(jobID string, value []byte) (err error) {
	_, err = dao.kv.Put(fmt.Sprintf(kvPatternJob, dao.cluster, jobID), string(value))
	return err
}

// RemoveJob ..
func (dao *DAO) RemoveJob(jobID string) (err error) {
	_, err = dao.kv.DeleteOne(fmt.Sprintf(kvPatternJob, dao.cluster, jobID))
	return err
}

// GetAllJobIDs ..
func (dao *DAO) GetAllJobIDs() (jobIDs []string, err error) {
	jobIDs = []string{}
	dirPath := fmt.Sprintf(kvPatternJobsDir, dao.cluster)
	err = dao.kv.GetWithPrefix(dirPath,
		func(fullPath string, rowID string, value []byte) bool {
			jobid := rowID
			jobIDs = append(jobIDs, jobid)
			return true
		})

	return jobIDs, err
}

// GetAllJobs ..
func (dao *DAO) GetAllJobs() (jobs map[string]Job, err error) {
	jobs = make(map[string]Job)
	dirPath := fmt.Sprintf(kvPatternJobsDir, dao.cluster)
	err = dao.kv.GetWithPrefix(dirPath,
		func(fullPath string, rowID string, value []byte) bool {
			jobid := rowID
			job := Job{ID: jobid, Data: value}
			jobs[jobid] = job
			return true
		})

	return jobs, err
}

// WatchJobs ..
func (dao *DAO) WatchJobs(handler func(jobid string, data []byte)) (watcher *kv.Watcher) {
	dirPath := fmt.Sprintf(kvPatternJobsDir, dao.cluster)
	watcher = dao.kv.WatchWithPrefix(dirPath,
		func(eventType kv.EventType, fullPath string, rowID string, value []byte) {
			jobid := rowID
			handler(jobid, value)
		})
	return watcher
}
