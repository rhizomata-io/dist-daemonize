package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rhizomata-io/dist-daemonize/kernel"
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-daemonize/protocol"
)

// BuiltinService ..
type BuiltinService struct {
	kernel *kernel.Kernel
}

func (service BuiltinService) health(context *gin.Context) {
	checkFrom := context.GetHeader("Check-From")
	fmt.Println("checkFrom : ", checkFrom)
	// fmt.Println("service.kernel.ID() : ", service.kernel.ID())
	context.Header(protocol.HeaderKernelID, service.kernel.ID())
	context.Writer.WriteString(service.kernel.ID())
	context.Writer.Flush()
}

func (service BuiltinService) addJob(context *gin.Context) {
	data, err := context.GetRawData()
	if err != nil {
		context.Status(http.StatusBadRequest)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
		return
	}

	job := service.kernel.GetJobManager().AddJob(job.NewJob(data))
	data, err = json.Marshal(job)
	if err != nil {
		context.Status(http.StatusInternalServerError)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
		return
	}
	context.Writer.Write(data)
	context.Writer.Flush()
}

func (service BuiltinService) addJobWithID(context *gin.Context) {
	jobid := context.Param("jobid")
	data, err := context.GetRawData()
	if err != nil {
		context.Status(http.StatusBadRequest)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
		return
	}

	job := service.kernel.GetJobManager().AddJob(job.NewWithID(jobid, data))
	data, err = json.Marshal(job)
	if err != nil {
		context.Status(http.StatusInternalServerError)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
		return
	}
	context.Writer.Write(data)
	context.Writer.Flush()
}

func (service BuiltinService) removeJob(context *gin.Context) {
	data, err := context.GetRawData()
	if err != nil {
		context.Status(http.StatusBadRequest)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
		return
	}

	err = service.kernel.GetJobManager().RemoveJob(string(data))
	if err != nil {
		context.Status(http.StatusInternalServerError)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
		return
	}
	context.Writer.WriteString("ok")
	context.Writer.Flush()
}
