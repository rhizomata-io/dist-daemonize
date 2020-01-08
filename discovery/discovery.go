package discovery

import (
	"log"

	"github.com/rhizomata-io/dist-daemonize/kernel"
	"github.com/rhizomata-io/dist-daemonize/kernel/cluster"
	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
)

// Discovery ...
type Discovery struct {
	kernel     *kernel.Kernel
	dao        *DAO
	watcher    *kv.Watcher
	jobMembMap map[string]*cluster.Member
}

//New create new Discovery
func New(kernel *kernel.Kernel) (discovery *Discovery) {
	dao := &DAO{cluster: kernel.GetClusterManager().GetCluster().Name(), kv: kernel.GetKV()}
	discovery = &Discovery{kernel: kernel, dao: dao, jobMembMap: map[string]*cluster.Member{}}

	kernel.AddOnJobDistributed(discovery.OnJobDistributed)
	return discovery
}

// GetMemberByJob ...
func (discovery *Discovery) GetMemberByJob(jobid string) *cluster.Member {
	return discovery.jobMembMap[jobid]
}

// OnJobDistributed : kernel.Kernel.SetOnJobDistributed
func (discovery *Discovery) OnJobDistributed(membJobMap map[string][]string) {
	discovery.dao.PutDiscoveryInfo(membJobMap)
}

// Start ...
func (discovery *Discovery) watchDiscoveryInfo(membJobMap map[string][]string) {
	jobMembMap := map[string]*cluster.Member{}

	for memb, jobs := range membJobMap {
		member := discovery.kernel.GetClusterManager().GetCluster().GetMember(memb)
		if member == nil {
			log.Println("[ERROR-Discovery] Unknown member : ", memb)
			continue
		}
		for _, job := range jobs {
			jobMembMap[job] = member
		}
	}
	discovery.jobMembMap = jobMembMap
	log.Println("[INFO] Discovery Info::", membJobMap)
}

// Start ...
func (discovery *Discovery) Start() (err error) {
	watcher := discovery.dao.WatchDiscoveryInfo(discovery.watchDiscoveryInfo)
	discovery.watcher = watcher

	return err
}

// Stop ...
func (discovery *Discovery) Stop() (err error) {
	if discovery.watcher != nil {
		discovery.watcher.Stop()
	}
	return err
}
