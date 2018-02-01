package main

import "github.com/cloudtask/cloudtask-center/api"
import "github.com/cloudtask/cloudtask-center/etc"
import "github.com/cloudtask/cloudtask-center/server"
import "github.com/cloudtask/libtools/gounits/flocker"
import "github.com/cloudtask/libtools/gounits/logger"
import "github.com/cloudtask/libtools/gounits/rand"
import "github.com/cloudtask/libtools/gounits/system"

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"time"
)

//JobServer is exported
type JobServer struct {
	RetryStartup bool
	Locker       *flocker.FileLocker
	CenterServer *server.CenterServer
	APIServer    *api.Server
}

//AppCode is exported
var AppCode string

func init() {

	if appFile, err := exec.LookPath(os.Args[0]); err == nil {
		AppCode, _ = system.ReadFileMD5Code(appFile)
	}
}

//NewJobServer is exported
func NewJobServer() (*JobServer, error) {

	var filePath string
	flag.StringVar(&filePath, "f", "./etc/jobserver.yaml", "jobserver etc file.")
	flag.Parse()
	if err := etc.New(filePath); err != nil {
		return nil, err
	}

	logConfigs := etc.LoggerConfigs()
	if logConfigs == nil {
		return nil, fmt.Errorf("logger configs invalid.")
	}
	logger.OPEN(logConfigs)

	key, err := rand.UUIDFile("./jobserver.key") //服务器唯一标识文件
	if err != nil {
		return nil, err
	}

	var fLocker *flocker.FileLocker
	if pidFile := etc.PidFile(); pidFile != "" {
		fLocker = flocker.NewFileLocker(pidFile, 0)
	}

	centerServer, err := server.NewCenterServer(key)
	if err != nil {
		return nil, err
	}

	api.RegisterStore("AppCode", AppCode)
	api.RegisterStore("SystemConfig", etc.SystemConfig)
	api.RegisterStore("ServerKey", centerServer.Key)
	api.RegisterStore("NodeData", centerServer.Data)
	api.RegisterStore("CacheRepository", centerServer.CacheRepository)
	api.RegisterStore("Scheduler", centerServer.Scheduler)
	api.RegisterStore("MessageCache", centerServer.MessageCache)
	apiServer := api.NewServer(etc.SystemConfig.API.Hosts, etc.SystemConfig.API.EnableCors, nil)

	return &JobServer{
		RetryStartup: etc.RetryStartup(),
		Locker:       fLocker,
		CenterServer: centerServer,
		APIServer:    apiServer,
	}, nil
}

//Startup is exported
func (server *JobServer) Startup() error {

	var (
		err     error
		startCh chan bool = make(chan bool, 1)
	)

	go func(c <-chan bool) {
		select {
		case <-c:
			logger.INFO("[#main] API listening: %s", server.APIServer.ListenHosts())
			if err := server.APIServer.Startup(); err != nil {
				logger.ERROR("[#main#] API startup error, %s", err.Error())
				return
			}
		}
	}(startCh)

	for {
		if err != nil {
			if server.RetryStartup == false {
				return err
			}
			time.Sleep(time.Second * 10) //retry, after sleep 10 seconds.
		}

		server.Locker.Unlock()
		if err = server.Locker.Lock(); err != nil {
			logger.ERROR("[#main#] pidfile lock error, %s", err)
			continue
		}

		if err = server.CenterServer.Startup(startCh); err != nil {
			logger.ERROR("[#main#] start server failure.")
			continue
		}
		break
	}
	logger.INFO("[#main#] jobserver started.")
	logger.INFO("[#main#] key:%s", server.CenterServer.Key)
	close(startCh)
	return nil
}

//Stop is exported
func (server *JobServer) Stop() error {

	server.Locker.Unlock()
	if err := server.CenterServer.Stop(); err != nil {
		logger.ERROR("[#main#] stop server failure.")
		return err
	}
	logger.INFO("[#main#] jobserver stoped.")
	logger.CLOSE()
	return nil
}
