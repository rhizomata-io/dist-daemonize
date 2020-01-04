package api

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/rhizomata-io/dist-daemonize/kernel"
	"github.com/rhizomata-io/dist-daemonize/protocol"
)

// Server ..
type Server struct {
	router         *gin.Engine
	builtinService *BuiltinService
	err            chan error
}

// NewServer create new API Server
func NewServer(kernel *kernel.Kernel) (server *Server) {
	server = new(Server)
	server.err = make(chan error)
	server.builtinService = &BuiltinService{kernel: kernel}
	server.router = gin.Default()

	v1 := server.router.Group(protocol.V1Path)
	{
		v1.HEAD(protocol.HealthPath, server.builtinService.health)
		v1.GET(protocol.HealthPath, server.builtinService.health)
		v1.POST(protocol.AddJobPath, server.builtinService.addJob)
		v1.POST(protocol.AddJobWithIDPath, server.builtinService.addJobWithID)
		v1.POST(protocol.RemoveJobPath, server.builtinService.removeJob)
	}

	return server
}

func (server *Server) Error() <-chan error {
	return server.err
}

// Start ..
func (server *Server) Start(listenAddress string) {
	go func() {
		err := server.router.Run(listenAddress)
		if err != nil {
			log.Fatal("Cannot Start API Server")
		}
	}()
}

// Group : delegate *gin.RouterGroup.Group
func (server *Server) Group(relativePath string, handlers ...gin.HandlerFunc) *gin.RouterGroup {
	return server.router.Group(relativePath, handlers...)
}
