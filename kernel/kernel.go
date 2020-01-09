package kernel

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/rhizomata-io/dist-daemonize/kernel/cluster"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
)

const (
	fileNameKernelID = ".kernel"
)

// Kernel ..
type Kernel struct {
	config            *config.RunOptions
	id                string
	kv                kv.KV
	clusterManager    *cluster.Manager
	jobManager        *job.Manager
	jobOrganizer      job.Organizer
	workerManager     *worker.Manager
	rootWorkerFactory *worker.AbstractWorkerFactory
	onJobDistributed  []func(membJobMap map[string][]string)
}

// New ..
func New(config *config.RunOptions) (kernel *Kernel, err error) {
	kernel = new(Kernel)
	kernel.config = config
	workerFactory := worker.NewAbstractWorkerFactory("_root")
	kernel.rootWorkerFactory = workerFactory
	err = kernel.initialize(workerFactory)
	kernel.onJobDistributed = []func(membJobMap map[string][]string){}

	return kernel, err
}

//RegisterWorkerFactory register worker.Factory
func (kernel *Kernel) RegisterWorkerFactory(factory worker.Factory) {
	kernel.rootWorkerFactory.AddFactory(factory)
}

// SetJobOrganizer : Set JobOrganizer
func (kernel *Kernel) SetJobOrganizer(jobOrganizer job.Organizer) {
	kernel.jobOrganizer = jobOrganizer
}

// AddOnJobDistributed : Add onJobDistributed event handler
func (kernel *Kernel) AddOnJobDistributed(onJobDistributed func(membJobMap map[string][]string)) {
	kernel.onJobDistributed = append(kernel.onJobDistributed, onJobDistributed)
}

// SetHealthCheckDelegator ..
func (kernel *Kernel) SetHealthCheckDelegator(healthCheckDelegator func(serviceUrl string) (string, error)) {
	kernel.GetClusterManager().SetHealthCheckDelegator(func(memb *cluster.Member) bool {
		remoteID, err := healthCheckDelegator(memb.ServiceURL)
		if err == nil && memb.ID == remoteID {
			return true
		}
		log.Println("[WARN-HealthCheck] Member["+memb.Name+":"+memb.ID+"] is not healthy. remote:", remoteID)
		return false
	})
}

func (kernel *Kernel) initialize(workerFactory worker.Factory) (err error) {
	if _, err := os.Stat(kernel.config.DataDir); err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(kernel.config.DataDir, os.ModePerm)
		} else {
			log.Println("[FATAL] Read local kernel data directory::", kernel.config.DataDir, err)
			return err
		}
	}
	localFilePath := filepath.Join(kernel.config.DataDir, fileNameKernelID)
	kernelidBytes, err := ioutil.ReadFile(localFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("[FATAL] Read local kernel id file::", localFilePath, err)
			return err
		}
	}

	if kernelidBytes == nil || len(kernelidBytes) == 0 {
		uuid := uuid.New()
		kernelidBytes = []byte(uuid.String())
		err := ioutil.WriteFile(localFilePath, kernelidBytes, 777)
		if err != nil {
			log.Println("[FATAL] Write local kernel id file::", localFilePath, err)
			return err
		}
	}

	kernel.id = string(kernelidBytes)
	log.Println("[BC-INFO] Kernel Instance ID : ", kernel.id)

	if kernel.kv != nil {
		kernel.kv.Close()
	}

	kv, err := kv.New(kernel.config.EtcdUrls)

	if err != nil {
		log.Println("[FATAL] Cannot Connect to KV Store(ETCD) : ", err)
		return err
	}

	log.Println("[INFO-Kernel] Connect to KV Store : ", kernel.config.EtcdUrls)

	kernel.kv = kv

	if kernel.clusterManager != nil {
		kernel.clusterManager.Dispose()
	}

	kernel.clusterManager = cluster.NewManager(kernel.id, *kernel.config, kernel.kv)

	kernel.jobManager = job.NewManager(kernel.config.Cluster, kernel.id, kernel.kv)

	kernel.workerManager = worker.NewManager(kernel.config.Cluster, kernel.id, kernel.kv, workerFactory)

	return err
}

// ID get ID
func (kernel *Kernel) ID() string {
	return kernel.id
}

// GetKV kernel.kv
func (kernel *Kernel) GetKV() kv.KV {
	return kernel.kv
}

// GetClusterManager kernel.clusterManager
func (kernel *Kernel) GetClusterManager() *cluster.Manager {
	return kernel.clusterManager
}

// GetJobManager kernel.jobManager
func (kernel *Kernel) GetJobManager() *job.Manager {
	return kernel.jobManager
}

// GetWorkerManager kernel.workerManager
func (kernel *Kernel) GetWorkerManager() *worker.Manager {
	return kernel.workerManager
}

// Start ..
func (kernel *Kernel) Start() (err error) {
	if kernel.jobOrganizer == nil {
		kernel.jobOrganizer = job.NewSimpleOrganizer()
		log.Println("[WARN-Kernel] JobOrganizer is not specified, so SimpleOrganizer is set.")
	}

	kernel.clusterManager.SetMemberChangeHandler(func(aliveMembers []string) {
		fmt.Println("********** Member Changed **********")
		fmt.Println("   ** aliveMembers::", aliveMembers)

		allJobs, err := kernel.jobManager.GetAllJobs()
		if err != nil {
			log.Println("[ERROR-Kernel] GetAllJobs ", err)
			allJobs = make(map[string]job.Job)
		}

		fmt.Println("   ** allJobs::", allJobs)

		kernel.distributeMemberJobs(allJobs, aliveMembers)
	})

	kernel.clusterManager.Start()

	kernel.jobManager.SetMembJobWatchHandler(func(jobids []string) {
		jobs := make(map[string]*job.Job)
		for _, id := range jobids {
			j, _ := kernel.jobManager.GetJob(id)
			jobs[id] = &j
		}

		kernel.workerManager.SetJobs(jobs)
	})

	kernel.jobManager.SetJobWatchHandler(func(job *job.Job) {
		log.Println("[WARN-Kernel] Job changed.", job)
		if kernel.clusterManager.IsLeader() {
			aliveMembers := kernel.GetClusterManager().GetCluster().GetAliveMemberIDs()
			allJobs, err := kernel.jobManager.GetAllJobs()
			if err != nil {
				log.Println("[ERROR-Kernel] GetAllJobs ", err)
			}
			kernel.distributeMemberJobs(allJobs, aliveMembers)
		}
	})
	kernel.jobManager.Start()

	log.Println("[INFO-Kernel] Kernel Starts. ", kernel.config)
	return err
}

// Stop ..
func (kernel *Kernel) Stop() {
	if kernel.kv != nil {
		kernel.kv.Close()
		kernel.kv = nil
	}
	if kernel.clusterManager != nil {
		kernel.clusterManager.Dispose()
		kernel.clusterManager = nil
	}

	if kernel.jobManager != nil {
		kernel.jobManager.Dispose()
		kernel.jobManager = nil
	}

	kernel.workerManager.Dispose()
}

//for leader only
func (kernel *Kernel) distributeMemberJobs(allJobs map[string]job.Job, aliveMembers []string) {
	membJobMap, err := kernel.jobManager.GetAllMemberJobIDs()

	if err != nil {
		log.Println("[ERROR-Kernel] GetAllMemberJobIDs ", err)
		membJobMap = make(map[string][]string)
	}

	var buffer bytes.Buffer

	buffer.WriteString("[WARN-Kernel] Before Organizing::\n")
	buffer.WriteString(fmt.Sprintln("    all jobs:", len(allJobs)))
	buffer.WriteString("    member jobs:\n")

	for k, v := range membJobMap {
		memb := kernel.GetClusterManager().GetCluster().GetMember(k)
		buffer.WriteString(fmt.Sprintln("      - ", memb.Name, " ## count:", len(v), v))
	}

	log.Println(buffer.String())

	membJobMap, err = kernel.jobOrganizer.Distribute(allJobs, aliveMembers, membJobMap)

	buffer.WriteString("[WARN-Kernel] After Organizing::\n")
	for k, v := range membJobMap {
		memb := kernel.GetClusterManager().GetCluster().GetMember(k)
		buffer.WriteString(fmt.Sprintln("      - ", memb.Name, " ## count:", len(v), v))
	}

	log.Println(buffer.String())

	for memb, jobs := range membJobMap {
		kernel.jobManager.SetMemberJobIDs(memb, jobs)
	}

	for _, handler := range kernel.onJobDistributed {
		handler(membJobMap)
	}
}
