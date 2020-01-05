package discovery

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rhizomata-io/dist-daemonize/api"
	"github.com/rhizomata-io/dist-daemonize/protocol"
)

const (
	discoveryPath = protocol.V1Path + "/discovery"
)

// APIService ...
type APIService struct {
	discovery *Discovery
}

//SupportAPI create new APIService and apply to api.Server
func SupportAPI(discovery *Discovery, apiServer *api.Server) (api *APIService) {
	api = &APIService{discovery: discovery}
	discoveryGroup := apiServer.Group(discoveryPath)
	{
		discoveryGroup.GET("/getbyjob/:jobid", api.getByJob)
		discoveryGroup.GET("/getalljobs", api.getAllJobs)
	}
	return api
}

// /api/v1/discovery/getbyjob/:jobid
func (api *APIService) getByJob(context *gin.Context) {
	jobid := context.Param("jobid")
	memb := api.discovery.GetMemberByJob(jobid)
	if memb != nil {
		data, err := json.Marshal(memb)
		if err != nil {
			log.Println("[ERROR] Marshal member data ", err)
			context.Status(http.StatusInternalServerError)
			context.Writer.WriteString(err.Error())
			context.Writer.Flush()
		} else {
			context.Writer.Write(data)
			context.Writer.Flush()
		}
	} else {
		context.Status(http.StatusNotFound)
		context.Writer.WriteString("Unknown Job :" + jobid)
		context.Writer.Flush()
	}
}

// /api/v1/discovery/getbyjob/:jobid
func (api *APIService) getAllJobs(context *gin.Context) {
	jobs := []string{}
	for job := range api.discovery.jobMembMap {
		jobs = append(jobs, job)
	}
	data, err := json.Marshal(jobs)
	if err != nil {
		log.Println("[ERROR] Marshal Job List ", err)
		context.Status(http.StatusInternalServerError)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
	} else {
		context.Writer.Write(data)
		context.Writer.Flush()
	}
}
