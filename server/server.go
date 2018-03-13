package server

import "github.com/cloudtask/cloudtask-center/cache"
import "github.com/cloudtask/cloudtask-center/etc"
import "github.com/cloudtask/cloudtask-center/notify"
import "github.com/cloudtask/cloudtask-center/scheduler"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gzkwrapper"
import "github.com/cloudtask/libtools/gounits/logger"

import (
	"fmt"
	"time"
)

//CenterServer is exported
type CenterServer struct {
	Key             string
	ConfigPath      string
	Data            *gzkwrapper.NodeData
	Master          *gzkwrapper.Server
	NotifySender    *notify.NotifySender
	CacheRepository *cache.CacheRepository
	Scheduler       *scheduler.Scheduler
	MessageCache    *models.MessageCache
	stopCh          chan struct{}
	gzkwrapper.INodeNotifyHandler
}

//NewCenterServer is exported
func NewCenterServer(key string) (*CenterServer, error) {

	clusterConfigs := etc.ClusterConfigs()
	if clusterConfigs == nil {
		return nil, fmt.Errorf("cluster configs invalid.")
	}

	cacheConfigs := etc.CacheConfigs()
	if cacheConfigs == nil {
		return nil, fmt.Errorf("cache configs invalid.")
	}

	schedulerConfigs := etc.SchedulerConfigs()
	if schedulerConfigs == nil {
		return nil, fmt.Errorf("scheduler configs invalid.")
	}

	server := &CenterServer{
		Key:        key,
		ConfigPath: clusterConfigs.Root + "/ServerConfig",
		stopCh:     make(chan struct{}),
	}

	master, err := gzkwrapper.NewServer(key, clusterConfigs, server)
	if err != nil {
		return nil, err
	}

	cacheConfigs.AllocHandlerFunc = server.OnAllocCacheHandlerFunc
	cacheConfigs.NodeHandlerFunc = server.OnNodeCacheHandlerFunc
	cacheConfigs.ReadLocationAllocFunc = server.readLocationAlloc
	cacheConfigs.ProcLocationAllocFunc = server.procLocationAlloc
	cacheRepository, err := cache.NewCacheRepository(cacheConfigs)
	if err != nil {
		return nil, err
	}

	server.Master = master
	server.Data = master.Data
	server.CacheRepository = cacheRepository
	server.NotifySender = notify.NewNotifySender(etc.Notifications())
	server.Scheduler = scheduler.NewScheduler(schedulerConfigs, server.CacheRepository)
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

		recoveryInterval, err := time.ParseDuration(server.Scheduler.AllocRecovery)
		if err != nil {
			recoveryInterval, _ = time.ParseDuration("320s")
		}

		if recoveryInterval != 0 {
			go server.monitorAllocLoop(recoveryInterval)
		}
	}()

	if err = server.Master.Open(); err != nil {
		logger.ERROR("[#server#] cluster zookeeper open failure, %s", err)
		return err
	}

	if etc.UseServerConfig() {
		server.initServerConfig()
	}

	if err = server.CacheRepository.Open(); err != nil {
		logger.ERROR("[#server] storage driver open failure, %s", err)
		return err
	}

	startCh <- true
	logger.INFO("[#server] server initialize......")
	server.CacheRepository.InitLocalStorageLocations()
	server.Master.RefreshCache()
	return nil
}

//Stop is exported
func (server *CenterServer) Stop() error {

	close(server.stopCh)
	server.CacheRepository.Close()
	server.CacheRepository.Clear()
	server.closeServerConfig()
	server.Master.Clear()
	if err := server.Master.Close(); err != nil {
		logger.ERROR("[#server] cluster zookeeper close error, %s", err.Error())
		return err
	}
	return nil
}

//initServerConfig is exported
//initialize server congfig and watching zk config node path.
func (server *CenterServer) initServerConfig() {

	//watch server config path.
	err := server.Master.WatchOpen(server.ConfigPath, server.OnSeverConfigsWatchHandlerFunc)
	if err != nil {
		logger.WARN("[#server] init serverConfig error %s, used local configs.", err)
		return
	}

	//read config data.
	data, err := server.Master.Get(server.ConfigPath)
	if err != nil {
		logger.WARN("[#server] get serverConfig error %s, used local configs.", err)
		return
	}
	//save data to etc serverConfig.
	server.RefreshServerConfig(data)
	logger.INFO("[#server#] inited server config.")
}

//closeServerConfig is exported
func (server *CenterServer) closeServerConfig() {

	server.Master.WatchClose(server.ConfigPath)
}

//RefreshServerConfig is exported
//save serverConfig, re-set to references objects.
func (server *CenterServer) RefreshServerConfig(data []byte) error {

	if etc.UseServerConfig() {
		if err := etc.SaveServerConfig(data); err != nil {
			return err
		}

		if cacheConfigs := etc.CacheConfigs(); cacheConfigs != nil {
			server.CacheRepository.SetStorageDriverConfigParameters(cacheConfigs.StorageDriverParameters)
		}
	}
	return nil
}

//readLocationAlloc is exported
//read location alloc data.
func (server *CenterServer) readLocationAlloc(location string) []byte {

	var (
		ret  bool
		err  error
		data []byte
	)

	allocPath := server.Master.Root + "/JOBS-" + location
	ret, err = server.Master.Exists(allocPath)
	if err != nil {
		logger.ERROR("[#server#] %s alloc check exists %s error, %s.", location, allocPath, err)
		return nil
	}

	if ret {
		if data, err = server.Master.Get(allocPath); err != nil {
			logger.ERROR("[#server#] %s alloc get data %s error, %s.", location, allocPath, err)
			return nil
		}
	}
	return data
}

func (server *CenterServer) postCacheAlloc(location string, data []byte) {

	var (
		ret bool
		err error
	)

	allocPath := server.Master.Root + "/JOBS-" + location
	ret, err = server.Master.Exists(allocPath)
	if err != nil {
		logger.ERROR("[#server#] post %s cache alloc exists error, %s.", location, err)
		return
	}

	if !ret {
		err = server.Master.Create(allocPath, data)
	} else {
		err = server.Master.Set(allocPath, data)
	}

	if err != nil {
		logger.ERROR("[#server#] post %s cache alloc error, %s.", location, err)
		return
	}
	logger.INFO("[#server#] post %s cache alloc successed...", location)
}

func (server *CenterServer) putCacheAlloc(location string, data []byte) {

	var (
		ret bool
		err error
	)

	allocPath := server.Master.Root + "/JOBS-" + location
	ret, err = server.Master.Exists(allocPath)
	if err != nil {
		logger.ERROR("[#server#] put %s cache alloc exists error, %s.", location, err)
		return
	}

	if !ret {
		logger.ERROR("[#server#] put %s cache alloc not found.", location)
		return
	}

	if err = server.Master.Set(allocPath, data); err != nil {
		logger.ERROR("[#server#] put %s cache alloc error, %s.", location, err)
		return
	}
	logger.INFO("[#server#] put %s cache alloc successed...", location)
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
						server.cleanDumpLocationAlloc(location, jobs, jobsAlloc)
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

//cleanDumpLocationAlloc is exported
//清扫任务分配表，将分配表中存在，而数据库中不存在或已关闭的任务从分配表删除
func (server *CenterServer) cleanDumpLocationAlloc(location string, jobs []*models.SimpleJob, jobsAlloc *models.JobsAlloc) {

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

func (server *CenterServer) procLocationAlloc(location string, addServers []*models.Server, delServers []*models.Server) {

	nodeStore := gzkwrapper.NewNodeStore()
	for _, addServer := range addServers {
		nodes := server.Master.GetNodes(location, addServer.IPAddr, addServer.Name)
		for key, node := range nodes {
			server.CacheRepository.CreateWorker(key, node)
			nodeStore.New[key] = node
		}
	}

	for _, delServer := range delServers {
		nodes := server.Master.GetNodes(location, delServer.IPAddr, delServer.Name)
		for key, node := range nodes {
			server.CacheRepository.RemoveWorker(key, node)
			nodeStore.Dead[key] = node
		}
	}

	if nodeStore.NewTotalSize() > 0 || nodeStore.DeadTotalSize() > 0 {
		server.Scheduler.QuickAlloc(nodeStore.New, nodeStore.Dead)
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
