package cache

import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/logger"

import (
	"sync"
)

//CloudDataClient is exported
type CloudDataClient struct {
	sync.RWMutex
	engine *Engine
}

//NewCloudDataClient is exported
func NewCloudDataClient(cloudPageSize int, serverConfig *models.ServerConfig) *CloudDataClient {

	return &CloudDataClient{
		engine: NewEngine(cloudPageSize, serverConfig),
	}
}

//SetServerConfig is exported
//setting serverConfig
func (client *CloudDataClient) SetServerConfig(serverConfig *models.ServerConfig) {

	client.engine.serverConfig = serverConfig
}

//GetLocationsName is exported
func (client *CloudDataClient) GetLocationsName() []string {

	names, err := client.engine.readLocationsName()
	if err != nil {
		logger.ERROR("[#cache#] engine read locations name error, %s", err.Error())
		return []string{}
	}
	return names
}

//GetLocation is exported
func (client *CloudDataClient) GetLocation(location string) *models.WorkLocation {

	client.RLock()
	defer client.RUnlock()
	workLocation, err := client.engine.getLocation(location)
	if err != nil {
		logger.ERROR("[#cache#] engine read location %s error, %s", location, err.Error())
		return nil
	}
	return workLocation
}

//GetLocationServer is exported
func (client *CloudDataClient) GetLocationServer(location string, key string) *models.Server {

	client.RLock()
	defer client.RUnlock()
	workLocation, err := client.engine.getLocation(location)
	if err != nil {
		logger.ERROR("[#cache#] engine read location %s server %s error, %s", location, key, err.Error())
		return nil
	}

	for _, server := range workLocation.Server {
		if server.Key == key {
			return server
		}
	}
	return nil
}

//GetLocationServers is exported
func (client *CloudDataClient) GetLocationServers(location string) []*models.Server {

	client.RLock()
	defer client.RUnlock()
	workLocation, err := client.engine.getLocation(location)
	if err != nil {
		logger.ERROR("[#cache#] engine read location %s servers error, %s", location, err.Error())
		return []*models.Server{}
	}
	return workLocation.Server
}

//CreateLocationServer is exported
func (client *CloudDataClient) CreateLocationServer(datacenter string, location string, key string, hostname string,
	ipaddr string, apiaddr string, os string, platform string) *models.Server {

	client.Lock()
	defer client.Unlock()
	workLocation, err := client.engine.getLocation(location)
	if err != nil {
		if err != ERR_RESOURCE_NOTFOUND {
			logger.ERROR("[#cache#] engine create server %s read location %s error, %s", key, location, err.Error())
			return nil
		}
	}

	server := &models.Server{
		Key:      key,
		Name:     hostname,
		IPAddr:   ipaddr,
		APIAddr:  apiaddr,
		OS:       os,
		Platform: platform,
		Status:   0,
	}

	added := false
	if err == ERR_RESOURCE_NOTFOUND {
		added = true
		if datacenter == "" { //当节点未配置数据中心时, 默认选用跨机房调度器
			datacenter = models.CLUSTER_CROSS_DC
		}
		workLocation = &models.WorkLocation{
			Location: location,
			Cluster:  datacenter,
			Group:    []*models.Group{},
			Server:   []*models.Server{server},
		}
	} else {
		found := false
		for _, s := range workLocation.Server {
			if s.Key == server.Key {
				server = s //若存在，则返回已经存在的server对象，不替换.
				found = true
				break
			}
		}
		if !found {
			added = true
			workLocation.Server = append(workLocation.Server, server)
		}
	}

	if added {
		err = client.engine.postLocation(workLocation)
		if err != nil {
			logger.ERROR("[#cache#] engine create server %s write location %s error, %s", key, location, err.Error())
			return nil
		}
	}
	return server
}

//ChangeLocationServer is exported
func (client *CloudDataClient) ChangeLocationServer(location string, key string, hostname string,
	ipaddr string, apiaddr string, os string, platform string) *models.Server {

	client.Lock()
	defer client.Unlock()
	workLocation, err := client.engine.getLocation(location)
	if err != nil {
		logger.ERROR("[#cache#] engine change server %s read location %s error, %s", key, location, err.Error())
		return nil
	}

	var server *models.Server
	found := false
	for i := range workLocation.Server {
		if workLocation.Server[i].Key == key {
			workLocation.Server[i].Name = hostname
			workLocation.Server[i].IPAddr = ipaddr
			workLocation.Server[i].APIAddr = apiaddr
			workLocation.Server[i].OS = os
			workLocation.Server[i].Platform = platform
			server = workLocation.Server[i]
			found = true
			break
		}
	}

	if found {
		err = client.engine.postLocation(workLocation)
		if err != nil {
			logger.ERROR("[#cache#] engine change server %s write location %s error, %s", key, location, err.Error())
			return nil
		}
	}
	return server
}

//RemoveLocationServer is exported
func (client *CloudDataClient) RemoveLocationServer(location string, key string) {

	client.Lock()
	defer client.Unlock()
	workLocation, err := client.engine.getLocation(location)
	if err != nil {
		logger.ERROR("[#cache#] engine remove server %s read location %s error, %s", key, location, err.Error())
		return
	}

	found := false
	for i, s := range workLocation.Server {
		if s.Key == key {
			workLocation.Server = append(workLocation.Server[:i], workLocation.Server[i+1:]...)
			found = true
			break
		}
	}

	if found {
		err = client.engine.postLocation(workLocation)
		if err != nil {
			logger.ERROR("[#cache#] engine remove server %s write location %s error, %s", key, location, err.Error())
		}
	}
}

//GetSimpleJobs is exported
func (client *CloudDataClient) GetSimpleJobs(query map[string][]string) []*models.SimpleJob {

	client.RLock()
	defer client.RUnlock()
	jobs, err := client.engine.readSimpleJobs(query)
	if err != nil {
		logger.ERROR("[#cache#] engine read simple jobs %+v error, %s", query, err.Error())
		return []*models.SimpleJob{}
	}
	return jobs
}

//GetJobs is exported
func (client *CloudDataClient) GetJobs(query map[string][]string) []*models.Job {

	client.RLock()
	defer client.RUnlock()
	jobs, err := client.engine.readJobs(query)
	if err != nil {
		logger.ERROR("[#cache#] engine read jobs %+v error, %s", query, err.Error())
		return []*models.Job{}
	}
	return jobs
}

//GetSimpleJob is exported
func (client *CloudDataClient) GetSimpleJob(jobid string) *models.SimpleJob {

	client.RLock()
	defer client.RUnlock()
	job, err := client.engine.getSimpleJob(jobid)
	if err != nil {
		logger.ERROR("[#cache#] engine get simple job %s error, %s", jobid, err.Error())
		return nil
	}
	return job
}

//GetJob is exported
func (client *CloudDataClient) GetJob(jobid string) *models.Job {

	client.RLock()
	defer client.RUnlock()
	job, err := client.engine.getJob(jobid)
	if err != nil {
		logger.ERROR("[#cache#] engine get job %s error, %s", jobid, err.Error())
		return nil
	}
	return job
}

//SetJob is exported
func (client *CloudDataClient) SetJob(job *models.Job) {

	client.Lock()
	defer client.Unlock()
	if err := client.engine.putJob(job); err != nil {
		logger.ERROR("[#cache#] engine set job %s error, %s", job.JobId, err.Error())
	}
}

//SetJobAction is exported
func (client *CloudDataClient) SetJobAction(apiaddr string, location string, jobid string, action string) {

	err := client.engine.putJobAction(apiaddr, location, jobid, action)
	if err != nil {
		logger.ERROR("[#cache#] engine set job action error, %s", err.Error())
	}
}
