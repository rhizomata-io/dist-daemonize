package dd

import (
	"log"

	"github.com/rhizomata-io/dist-daemonize/api"
	"github.com/rhizomata-io/dist-daemonize/discovery"
	"github.com/rhizomata-io/dist-daemonize/kernel"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
	"github.com/rhizomata-io/dist-daemonize/protocol"
)

// Daemonizer : Distributed daemonizer
type Daemonizer struct {
	runOptions *config.RunOptions
	kernel     *kernel.Kernel
	apiServer  *api.Server
	discovery  *discovery.Discovery
}

// Daemonize creates new Daemonizer
func Daemonize(runOptions *config.RunOptions) (daemonizer Daemonizer, err error) {
	kernel, err := kernel.New(runOptions)

	if err != nil {
		log.Println("Cannot Daemonize::", err)
		return daemonizer, err
	}

	kernel.SetHealthCheckDelegator(protocol.CheckHealth)

	apiServer := api.NewServer(kernel)
	daemonizer = Daemonizer{kernel: kernel, apiServer: apiServer, runOptions: runOptions}

	return daemonizer, err
}

//GetKernel get kernel
func (daemonizer *Daemonizer) GetKernel() *kernel.Kernel {
	return daemonizer.kernel
}

//GetAPIServer get apiServer
func (daemonizer *Daemonizer) GetAPIServer() *api.Server {
	return daemonizer.apiServer
}

//RegisterWorkerFactory delegates kernel.Kernel.RegisterWorkerFactory
func (daemonizer *Daemonizer) RegisterWorkerFactory(factory worker.Factory) {
	daemonizer.kernel.RegisterWorkerFactory(factory)
}

//SetJobOrganizer delegates kernel.Kernel.RegisterWorkerFactory
func (daemonizer *Daemonizer) SetJobOrganizer(jobOrganizer job.Organizer) {
	daemonizer.kernel.SetJobOrganizer(jobOrganizer)
}

//StartDiscovery : this method may be called only after Start
func (daemonizer *Daemonizer) StartDiscovery() {
	disc := discovery.New(daemonizer.kernel)
	daemonizer.discovery = disc
	discovery.SupportAPI(disc, daemonizer.apiServer)
	disc.Start()
}

//Start start kernel and api server
func (daemonizer *Daemonizer) Start() (err error) {
	err = daemonizer.kernel.Start()
	if err == nil {
		daemonizer.apiServer.Start(daemonizer.runOptions.GetServiceAddr())
	}
	return err
}

// Wait wait
func (daemonizer *Daemonizer) Wait() {
	<-daemonizer.apiServer.Error()
}

// Stop stop kernel
func (daemonizer *Daemonizer) Stop() {
	daemonizer.kernel.Stop()
}

//AddJobIfNotExists : add job if job id is not registered.
func (daemonizer *Daemonizer) AddJobIfNotExists(job job.Job) {
	jobManager := daemonizer.kernel.GetJobManager()
	if !jobManager.ContainsJob(job.ID) {
		jobManager.AddJob(job)
	}
}
