package server

import "github.com/cloudtask/cloudtask-center/api"
import "github.com/cloudtask/cloudtask-center/cache"
import "github.com/cloudtask/cloudtask-center/etc"
import "github.com/cloudtask/cloudtask-center/notify"
import "github.com/cloudtask/cloudtask-center/scheduler"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gzkwrapper"
import "github.com/cloudtask/libtools/gounits/logger"

import (
	"sync"
	"time"
)

//CenterServer is exported
type CenterServer struct {
	gzkwrapper.INodeNotifyHandler
	cache.ICacheRepositoryHandler
	Key             string
	ConfigPath      string
	Data            *gzkwrapper.NodeData
	Master          *gzkwrapper.Server
	NotifySender    *notify.NotifySender
	CacheRepository *cache.CacheRepository
	Scheduler       *scheduler.Scheduler
	MessageCache    *models.MessageCache
	stopCh          chan struct{}
}

//NewCenterServer is exported
func NewCenterServer(key string) (*CenterServer, error) {

	clusterArgs := etc.ClusterArgs()
	server := &CenterServer{
		Key:        key,
		ConfigPath: clusterArgs.Root + "/ServerConfigs",
		stopCh:     make(chan struct{}),
	}

	master, err := gzkwrapper.NewServer(key, clusterArgs, server)
	if err != nil {
		return nil, err
	}

	server.Master = master
	server.Data = master.Data
	server.NotifySender = notify.NewNotifySender(etc.GetNotifications())
	server.CacheRepository = cache.NewCacheRepository(etc.GetCacheRepositoryArgs(), etc.ServerConfig, server)
	server.Scheduler = scheduler.NewScheduler(etc.AllocMode(), server.CacheRepository)
	server.MessageCache = models.NewMessageCache()
	return server, nil
}

//Startup is exported
func (server *CenterServer) Startup(startCh chan<- bool) error {

	var err error
	defer func() {
		if err != nil {
			server.Master.Close()
			return
		}

		recoveryInterval, err := time.ParseDuration(etc.AllocRecovery())
		if err != nil {
			recoveryInterval, _ = time.ParseDuration("320s")
		}

		if recoveryInterval != 0 {
			go server.monitorAllocLoop(recoveryInterval)
		}
	}()

	if err = server.Master.Open(); err != nil {
		logger.ERROR("[#server#] master open error, %s", err)
		return err
	}

	if err = server.initServerConfig(); err != nil {
		logger.ERROR("[#server#] init server config error, %s", err)
		return err
	}

	startCh <- true
	logger.INFO("[#server] server initialize......")
	server.initCacheAlloc()
	server.Master.RefreshCache()
	return nil
}

//Stop is exported
func (server *CenterServer) Stop() error {

	close(server.stopCh)
	server.CacheRepository.Clear()
	server.closeServerConfig()
	server.Master.Clear()
	if err := server.Master.Close(); err != nil {
		logger.ERROR("[#server] master close error, %s", err.Error())
		return err
	}
	return nil
}

//initServerConfig is exported
//initialize server congfigs and watching zk config path.
func (server *CenterServer) initServerConfig() error {

	//watch server config path.
	if err := server.Master.WatchOpen(server.ConfigPath, server.OnSeverConfigsWatchHandlerFunc); err != nil {
		return err
	}

	//read config data.
	data, err := server.Master.Get(server.ConfigPath)
	if err != nil {
		return err
	}
	//save data to etc serverConfig.
	return server.RefreshServerConfig(data)
}

//closeServerConfig is exported
func (server *CenterServer) closeServerConfig() {

	server.Master.WatchClose(server.ConfigPath)
}

//RefreshServerConfig is exported
//save serverConfig, re-set to references objects.
func (server *CenterServer) RefreshServerConfig(data []byte) error {

	if err := etc.SaveServerConfig(data); err != nil {
		return err
	}

	api.RegisterStore("ServerConfig", etc.ServerConfig)
	server.CacheRepository.SetServerConfig(etc.ServerConfig)
	return nil
}

func (server *CenterServer) initCacheAlloc() {

	waitGroup := sync.WaitGroup{}
	locations := server.CacheRepository.GetLocationsName()
	waitGroup.Add(len(locations))
	for _, location := range locations {
		go func(locationName string) {
			var (
				ret         bool
				err         error
				allocBuffer []byte
			)
			allocPath := server.Master.Root + "/JOBS-" + locationName
			ret, err = server.Master.Exists(allocPath)
			if err == nil {
				if !ret {
					allocBuffer, _ = server.CacheRepository.MakeAllocBuffer()
					err = server.Master.Create(allocPath, allocBuffer)
				} else {
					allocBuffer, err = server.Master.Get(allocPath)
				}
			}
			if err != nil {
				logger.ERROR("[#server#] init alloc %s error, %s", allocPath, err.Error())
			} else {
				//init local location, set alloc last version.
				logger.INFO("[#server#] init alloc %s", allocPath)
				server.CacheRepository.InitAllocBuffer(locationName, allocBuffer)
			}
			waitGroup.Done()
		}(location)
	}
	waitGroup.Wait()
}

func (server *CenterServer) postCacheAlloc(location string, data []byte) {

	if len(data) > 0 {
		if err := server.Master.Set(server.Master.Root+"/JOBS-"+location, data); err != nil {
			logger.ERROR("[#server#] post %s cache alloc error, %s", location, err)
			return
		}
		logger.INFO("[#server#] post %s cache alloc successed...", location)
	}
}

func (server *CenterServer) removeCacheAlloc(location string) {

	err := server.Master.Remove(server.Master.Root + "/JOBS-" + location)
	if err != nil {
		logger.ERROR("[#server#] remove %s cache alloc error, %s", location, err)
		return
	}
	logger.INFO("[#server#] remove %s cache alloc successed...", location)
}

//monitorAllocLoop is exported
//定期监视分配表，按每个location进行分配表任务检查和清理.
func (server *CenterServer) monitorAllocLoop(recoveryInterval time.Duration) {

	for {
		runTicker := time.NewTicker(recoveryInterval)
		select {
		case <-runTicker.C:
			{
				runTicker.Stop()
				locations := server.CacheRepository.GetLocationsName()
				for _, location := range locations {
					jobsAlloc := server.CacheRepository.GetAlloc(location)
					if jobsAlloc != nil {
						jobs := server.CacheRepository.GetLocationSimpleJobs(location)
						server.Scheduler.RecoveryLocationAlloc(location, jobs)
						server.cleanLocationAlloc(location, jobs, jobsAlloc)
					}
				}
			}
		case <-server.stopCh:
			{
				runTicker.Stop()
				logger.INFO("[#server] monitor alloc loop exited.")
				return
			}
		}
	}
}

//cleanLocationAlloc is exported
//清扫任务分配表，将分配表中存在，而数据库中不存在或已关闭的任务从分配表删除
func (server *CenterServer) cleanLocationAlloc(location string, jobs []*models.SimpleJob, jobsAlloc *models.JobsAlloc) {

	var (
		found  = false
		jobIds = []string{}
	)

	for _, jobData := range jobsAlloc.Jobs {
		for _, job := range jobs {
			if job.JobId == jobData.JobId && job.Enabled == 1 {
				found = true
				break
			}
		}
		if !found {
			jobIds = append(jobIds, jobData.JobId)
		}
		found = false
	}

	if len(jobIds) > 0 {
		server.CacheRepository.RemoveAllocJobs(location, jobIds)
	}
}

func (server *CenterServer) postNodesWatchNotifyEvent(online gzkwrapper.NodesPair, offline gzkwrapper.NodesPair) {

	watchLocations := make(notify.WatchLocations)
	watchLocations = server.setWatchLocations(watchLocations, online, "Healthy")
	watchLocations = server.setWatchLocations(watchLocations, offline, "Disconnected")
	for _, watchLocation := range watchLocations {
		server.NotifySender.AddLocationServersEvent("cluster discovery servers state changed.", watchLocation)
	}
}

func (server *CenterServer) setWatchLocations(watchLocations notify.WatchLocations, nodes gzkwrapper.NodesPair, state string) notify.WatchLocations {

	for _, nodedata := range nodes {
		watchlocation, ret := watchLocations[nodedata.Location]
		if !ret {
			watchlocation = &notify.WatchLocation{
				Name:        nodedata.Location,
				ContactInfo: []string{},
				Servers:     []*notify.Server{},
			}
			watchLocations[nodedata.Location] = watchlocation
		}
		watchlocation.AddServer(nodedata.IpAddr, nodedata.HostName, state)
		groups := server.CacheRepository.GetLocationGroups(watchlocation.Name)
		for _, group := range groups {
			for _, owner := range group.Owners {
				watchlocation.AddContactInfo(owner)
			}
		}
	}
	return watchLocations
}
