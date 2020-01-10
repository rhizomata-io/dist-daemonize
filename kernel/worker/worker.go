package worker

import (
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
)

// Worker ..
type Worker interface {
	ID() string
	Start() error
	Stop() error
	IsStarted() bool
}

// Factory ..
type Factory interface {
	Name() string
	NewWorker(helper *Helper) (Worker, error)
}

// Helper ..
type Helper struct {
	cluster string
	id      string
	job     *job.Job
	kv      kv.KV
	dao     *DAO
	started bool
}

// NewHelper ..
func NewHelper(cluster string, id string, job *job.Job, kv kv.KV) *Helper {
	helper := Helper{cluster: cluster, id: id, job: job, kv: kv}
	helper.dao = &DAO{cluster: cluster, kv: kv}
	return &helper
}

// CreateChildHelper ...
func (helper *Helper) CreateChildHelper(subid string, job *job.Job) *Helper {
	helper2 := Helper{cluster: helper.cluster, id: helper.id + "-" + subid, job: job, kv: helper.kv}
	helper2.dao = helper.dao
	return &helper2
}

// ID get worker's id
func (helper *Helper) ID() string {
	return helper.id
}

// Job get worker's Job
func (helper *Helper) Job() *job.Job {
	return helper.job
}

// IsStarted whether worker is started
func (helper *Helper) IsStarted() bool {
	return helper.started
}

// KV get worker's KV
func (helper *Helper) KV() kv.KV {
	return helper.kv
}

// PutCheckpoint ..
func (helper *Helper) PutCheckpoint(checkpoint interface{}) error {
	return helper.dao.PutCheckpoint(helper.id, checkpoint)
}

// GetCheckpoint ..
func (helper *Helper) GetCheckpoint(checkpoint interface{}) error {
	return helper.dao.GetCheckpoint(helper.id, checkpoint)
}

// PutData ..
func (helper *Helper) PutData(topic string, rowID string, data string) error {
	return helper.dao.PutData(helper.id, rowID, topic, data)
}

// PutObject ..
func (helper *Helper) PutObject(topic string, rowID string, data interface{}) error {
	return helper.dao.PutObject(helper.id, topic, rowID, data)
}

// GetData ..
func (helper *Helper) GetData(topic string, rowID string) (data []byte, err error) {
	return helper.dao.GetData(helper.id, topic, rowID)
}

// GetObject ..
func (helper *Helper) GetObject(topic string, rowID string, data interface{}) error {
	return helper.dao.GetObject(helper.id, topic, rowID, data)
}

// GetDataList ..
func (helper *Helper) GetDataList(topic string, handler func(key string, value []byte)) error {
	return helper.dao.GetDataWithTopic(helper.id, topic, handler)
}

// WatchData ..
func (helper *Helper) WatchData(topic string, handler func(key string, value []byte)) *kv.Watcher {
	return helper.dao.WatchDataWithTopic(helper.id, topic, handler)
}

// DeleteData ..
func (helper *Helper) DeleteData(topic string, rowID string) error {
	return helper.dao.DeleteData(helper.id, topic, rowID)
}

// DeleteDataFullPath ..
func (helper *Helper) DeleteDataFullPath(key string) error {
	return helper.dao.DeleteDataFullPath(key)
}
