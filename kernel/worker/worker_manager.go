package worker

import (
	"errors"
	"log"

	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
)

// Manager ..
type Manager struct {
	cluster string
	localid string
	kv      kv.KV
	// workerFactoryMethod func(helper *Helper) (Worker, error)
	workerFactory Factory
	workers       map[string]Worker
}

// NewManager create Manager
func NewManager(cluster string, localid string, kv kv.KV,
	workerFactory Factory) *Manager {
	manager := Manager{cluster: cluster, localid: localid, kv: kv,
		workerFactory: workerFactory}
	manager.workers = make(map[string]Worker)
	return &manager
}

// Cluster get cluster name
func (manager *Manager) Cluster() string { return manager.cluster }

// LocalID get local kernel id
func (manager *Manager) LocalID() string { return manager.localid }

// KV get etcd kv
func (manager *Manager) KV() kv.KV { return manager.kv }

// ContainsWorker if worker id is registered.
func (manager *Manager) ContainsWorker(id string) bool {
	return manager.workers[id] != nil
}

// GetWorker get worker for id
func (manager *Manager) GetWorker(id string) Worker {
	return manager.workers[id]
}

// NewHelper ..
func (manager *Manager) NewHelper(job *job.Job) (helper *Helper) {
	helper = NewHelper(manager.cluster, manager.localid, job.ID, job, manager.kv)
	return helper
}

// registerWorker ..
func (manager *Manager) registerWorker(id string, job *job.Job) error {
	if manager.workers[id] != nil {
		return errors.New("Worker[" + id + "] is already registered. If you want register new one, DeregisterWorker first")
	}
	helper := manager.NewHelper(job)
	worker, err := manager.workerFactory.NewWorker(helper)
	if err != nil {
		log.Println("[ERROR] Cannot create worker ", err)
		return err
	}

	manager.workers[id] = worker
	err = worker.Start()
	return err
}

// deregisterWorker ..
func (manager *Manager) deregisterWorker(id string) error {
	worker := manager.workers[id]
	if worker == nil {
		return errors.New("Worker[" + id + "] is not registered.")
	}

	err := worker.Stop()

	if err == nil {
		delete(manager.workers, id)
	}

	return err
}

// Dispose ..
func (manager *Manager) Dispose() error {
	array := []string{}
	for id := range manager.workers {
		array = append(array, id)
	}

	for _, id := range array {
		manager.deregisterWorker(id)
	}

	return nil
}

// SetJobs ...
func (manager *Manager) SetJobs(jobs map[string]*job.Job) {
	log.Println("[WARN-WorkerManager] Set Jobs:", len(jobs))

	tempWorkers := make(map[string]Worker)
	newWorkers := make(map[string]Worker)

	for id, worker := range manager.workers {
		tempWorkers[id] = worker
	}

	for id, job := range jobs {
		worker := tempWorkers[id]
		if worker != nil {
			delete(tempWorkers, id)
		} else {
			helper := manager.NewHelper(job)
			worker2, err := manager.workerFactory.NewWorker(helper)
			if err != nil {
				log.Println("[ERROR-WorkerMan] Cannot create worker ", err)
				continue
			} else {
				worker = worker2
				log.Println("[WARN-WorkerMan] New Worker .....", id)
			}
		}

		newWorkers[id] = worker
	}
	// 제거된 worker 종료하기
	for id, worker := range tempWorkers {
		worker.Stop()
		log.Println("[WARN-WorkerMan] Dispose Worker .....", id)
	}

	manager.workers = newWorkers

	for id, worker := range manager.workers {
		if !worker.IsStarted() {
			go func(id string, worker Worker) {
				log.Println("[WARN-WorkerMan] New Worker Starting .....", id)
				worker.Start()
			}(id, worker)
		} else {
			log.Println("[WARN-WorkerMan] Remained Worker .....", id)
		}
	}
}
