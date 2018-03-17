package cache

import _ "github.com/cloudtask/cloudtask-center/cache/driver/mongo"
import _ "github.com/cloudtask/cloudtask-center/cache/driver/ngcloud"
import "github.com/cloudtask/cloudtask-center/cache/driver"
import "github.com/cloudtask/cloudtask-center/cache/driver/types"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/httpx"
import "github.com/cloudtask/libtools/gounits/logger"
import "github.com/cloudtask/libtools/gzkwrapper"

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"
)

//ReadLocationAllocFunc is exported
//read jobs alloc path real data.
type ReadLocationAllocFunc func(location string) []byte

//ProcLocationAllocFunc is exported
type ProcLocationAllocFunc func(location string, addServers []*models.Server, delServers []*models.Server)

//CacheConfigs is exported
type CacheConfigs struct {
	LRUSize                 int
	StorageBackend          types.Backend
	StorageDriverParameters types.Parameters
	AllocHandlerFunc        AllocCacheEventHandlerFunc
	NodeHandlerFunc         NodeCacheEventHandlerFunc
	ReadLocationAllocFunc   ReadLocationAllocFunc
	ProcLocationAllocFunc   ProcLocationAllocFunc
}

//CacheRepository is expotred
type CacheRepository struct {
	nodeStore             *NodeStore
	allocStore            *AllocStore
	localStorage          *LocalStorage
	client                *httpx.HttpClient
	storageDriver         driver.StorageDriver
	readLocationAllocFunc ReadLocationAllocFunc
	procLocationAllocFunc ProcLocationAllocFunc
}

//NewCacheRepository is expotred
func NewCacheRepository(configs *CacheConfigs) (*CacheRepository, error) {

	storageDriver, err := driver.NewDriver(configs.StorageBackend, configs.StorageDriverParameters)
	if err != nil {
		return nil, err
	}

	if configs.LRUSize <= 0 {
		configs.LRUSize = DefaultLRUSize
	}

	client := httpx.NewClient().
		SetTransport(&http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
			DisableKeepAlives:     false,
			MaxIdleConns:          25,
			MaxIdleConnsPerHost:   25,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   http.DefaultTransport.(*http.Transport).TLSHandshakeTimeout,
			ExpectContinueTimeout: http.DefaultTransport.(*http.Transport).ExpectContinueTimeout,
		})

	return &CacheRepository{
		nodeStore:             NewNodeStore(configs.NodeHandlerFunc),
		allocStore:            NewAllocStore(configs.AllocHandlerFunc),
		localStorage:          NewLocalStorage(configs.LRUSize),
		client:                client,
		storageDriver:         storageDriver,
		readLocationAllocFunc: configs.ReadLocationAllocFunc,
		procLocationAllocFunc: configs.ProcLocationAllocFunc,
	}, nil
}

//Open is exported
func (cacheRepository *CacheRepository) Open() error {

	return cacheRepository.storageDriver.Open()
}

//Close is exported
func (cacheRepository *CacheRepository) Close() {

	cacheRepository.storageDriver.Close()
}

//SetStorageDriverConfigParameters is exported
func (cacheRepository *CacheRepository) SetStorageDriverConfigParameters(parameters types.Parameters) {

	cacheRepository.storageDriver.SetConfigParameters(parameters)
}

//Clear is expotred
func (cacheRepository *CacheRepository) Clear() {

	cacheRepository.nodeStore.ClearWorkers()
	cacheRepository.allocStore.ClearAlloc()
	cacheRepository.localStorage.Clear()
}

//SetLocalStorageLocation is exported
func (cacheRepository *CacheRepository) setLocalStorageLocation(location string) {

	workLocation := cacheRepository.storageDriver.GetLocation(location)
	if workLocation != nil {
		cacheRepository.localStorage.SetLocation(workLocation)
	}
}

//RemoveLocalStorageLocation is exported
func (cacheRepository *CacheRepository) removeLocalStorageLocation(location string) {

	cacheRepository.localStorage.RemoveLocation(location)
}

//InitLocalStorageLocations is exported
func (cacheRepository *CacheRepository) InitLocalStorageLocations() {

	cacheRepository.localStorage.Clear()
	locationsName := cacheRepository.storageDriver.GetLocationsName()
	for _, location := range locationsName {
		if strings.TrimSpace(location) != "" {
			cacheRepository.CreateLocationAlloc(location)
		}
	}
}

//readLocatinAlloc is exported
func (cacheRepository *CacheRepository) readLocatinAlloc(location string) []byte {

	data := cacheRepository.readLocationAllocFunc(location)
	if data == nil {
		jobsAlloc := &models.JobsAlloc{
			Version: 1,
			Jobs:    make([]*models.JobData, 0),
		}
		buffer := bytes.NewBuffer([]byte{})
		if err := json.NewEncoder(buffer).Encode(jobsAlloc); err != nil {
			return nil
		}
		data = buffer.Bytes()
	}
	return data
}

//CreateLocationAlloc is exported
func (cacheRepository *CacheRepository) CreateLocationAlloc(location string) {

	cacheRepository.setLocalStorageLocation(location)
	workLocation := cacheRepository.localStorage.GetLocation(location)
	if workLocation != nil {
		if data := cacheRepository.readLocatinAlloc(location); data != nil {
			cacheRepository.allocStore.CreateAlloc(location, data)
			if cacheRepository.HasAlloc(location) {
				cacheRepository.procLocationAllocFunc(location, workLocation.Server, []*models.Server{})
			}
		}
	}
}

//ChangeLocationAlloc is exported
func (cacheRepository *CacheRepository) ChangeLocationAlloc(location string) {

	originServers := cacheRepository.GetLocationServers(location)
	cacheRepository.setLocalStorageLocation(location)
	workLocation := cacheRepository.localStorage.GetLocation(location)
	if workLocation != nil {
		addServers := []*models.Server{}
		for _, server := range workLocation.Server {
			if ret := containsServer(server, originServers); !ret {
				addServers = append(addServers, server)
			}
		}
		delServers := []*models.Server{}
		for _, server := range originServers {
			if ret := containsServer(server, workLocation.Server); !ret {
				delServers = append(delServers, server)
			}
		}
		if cacheRepository.HasAlloc(location) {
			cacheRepository.procLocationAllocFunc(location, addServers, delServers)
		}
	}
}

//RemoveLocationAlloc is exported
func (cacheRepository *CacheRepository) RemoveLocationAlloc(location string) {

	cacheRepository.nodeStore.RemoveLocation(location)
	cacheRepository.allocStore.RemoveAlloc(location)
	cacheRepository.removeLocalStorageLocation(location)
}

//GetAllocData is exported
func (cacheRepository *CacheRepository) GetAllocData(location string) *models.JobsAllocData {

	jobsAlloc := cacheRepository.GetAlloc(location)
	if jobsAlloc == nil {
		return nil
	}

	jobsAllocData := &models.JobsAllocData{
		Location: location,
		Version:  jobsAlloc.Version,
		Data:     []*models.JobDataInfo{},
	}

	for _, jobData := range jobsAlloc.Jobs {
		jobDataInfo := &models.JobDataInfo{}
		jobDataInfo.JobId = jobData.JobId
		jobDataInfo.Key = jobData.Key
		jobDataInfo.Version = jobData.Version
		if worker := cacheRepository.GetWorker(jobData.Key); worker != nil {
			jobDataInfo.IpAddr = worker.IPAddr
			jobDataInfo.HostName = worker.Name
		}
		if job := cacheRepository.GetSimpleJob(jobData.JobId); job != nil {
			jobDataInfo.JobName = job.Name
		}
		jobsAllocData.Data = append(jobsAllocData.Data, jobDataInfo)
	}
	return jobsAllocData
}

//HasAlloc is exported
func (cacheRepository *CacheRepository) HasAlloc(location string) bool {

	return cacheRepository.allocStore.HasAlloc(location)
}

//HasAllocJobId is exported
func (cacheRepository *CacheRepository) HasAllocJobId(location string, jobId string) bool {

	return cacheRepository.allocStore.HasAllocJobId(location, jobId)
}

//GetAlloc is exported
func (cacheRepository *CacheRepository) GetAlloc(location string) *models.JobsAlloc {

	return cacheRepository.allocStore.GetAlloc(location)
}

//GetAllocJobIds is exported
func (cacheRepository *CacheRepository) GetAllocJobIds(location string, key string) []string {

	return cacheRepository.allocStore.GetAllocJobIds(location, key)
}

//GetAllocJob is exported
func (cacheRepository *CacheRepository) GetAllocJob(location string, jobId string) *models.JobData {

	return cacheRepository.allocStore.GetAllocJob(location, jobId)
}

//CreateAllocJob is exported
func (cacheRepository *CacheRepository) CreateAllocJob(location string, key string, jobId string) {

	cacheRepository.allocStore.CreateAllocJob(location, key, jobId)
}

//RemoveAllocJob is exported
func (cacheRepository *CacheRepository) RemoveAllocJob(location string, jobId string) {

	cacheRepository.allocStore.RemoveAllocJob(location, jobId)
}

//RemoveAllocJobs is exported
func (cacheRepository *CacheRepository) RemoveAllocJobs(location string, jobIds []string) {

	cacheRepository.allocStore.RemoveAllocJobs(location, jobIds)
}

//SetAllocJobsKey is exported
func (cacheRepository *CacheRepository) SetAllocJobsKey(location string, jobs map[string]string) {

	cacheRepository.allocStore.SetAllocJobsKey(location, jobs)
}

//UpdateAllocJob is exported
func (cacheRepository *CacheRepository) UpdateAllocJob(location string, jobId string) {

	cacheRepository.UpdateAllocJobs(location, []string{jobId})
}

//UpdateAllocJobs is exported
func (cacheRepository *CacheRepository) UpdateAllocJobs(location string, jobIds []string) {

	cacheRepository.allocStore.UpdateAllocJobs(location, jobIds)
}

//GetLocationsName is exported
func (cacheRepository *CacheRepository) GetLocationsName() []string {

	return cacheRepository.localStorage.GetLocationsName()
}

//GetLocation is exported
func (cacheRepository *CacheRepository) GetLocation(location string) *models.WorkLocation {

	return cacheRepository.localStorage.GetLocation(location)
}

//GetLocationServers is exported
func (cacheRepository *CacheRepository) GetLocationServers(location string) []*models.Server {

	return cacheRepository.localStorage.GetLocationServers(location)
}

//GetLocationGroups is exported
func (cacheRepository *CacheRepository) GetLocationGroups(location string) []*models.Group {

	return cacheRepository.localStorage.GetLocationGroups(location)
}

//GetLocationGroup is exported
func (cacheRepository *CacheRepository) GetLocationGroup(location string, groupid string) *models.Group {

	return cacheRepository.localStorage.GetLocationGroup(location, groupid)
}

//GetLocationSimpleJobs is exported
func (cacheRepository *CacheRepository) GetLocationSimpleJobs(location string) []*models.SimpleJob {

	return cacheRepository.storageDriver.GetLocationSimpleJobs(location)
}

//GetSimpleJob is exported
func (cacheRepository *CacheRepository) GetSimpleJob(jobid string) *models.SimpleJob {

	return cacheRepository.storageDriver.GetSimpleJob(jobid)
}

//GetRawJob is exported
func (cacheRepository *CacheRepository) GetRawJob(jobid string) *models.Job {

	job := cacheRepository.storageDriver.GetJob(jobid)
	if job != nil {
		cacheRepository.localStorage.AddJob(job)
	}
	return job
}

//GetJob is exported
func (cacheRepository *CacheRepository) GetJob(jobid string) *models.Job {

	job := cacheRepository.localStorage.GetJob(jobid)
	if job == nil {
		job = cacheRepository.GetRawJob(jobid)
	}
	return job
}

//RemoveJob is exported
func (cacheRepository *CacheRepository) RemoveJob(jobid string) {

	cacheRepository.localStorage.RemoveJob(jobid)
}

//RemoveJobs is exported
func (cacheRepository *CacheRepository) RemoveJobs(jobIds []string) {

	for _, jobid := range jobIds {
		cacheRepository.RemoveJob(jobid)
	}
}

//SetJobAction is exported
func (cacheRepository *CacheRepository) SetJobAction(location string, jobid string, action string) {

	logger.INFO("[#cache#] set job action, %s %s", jobid, action)
	job := cacheRepository.GetSimpleJob(jobid)
	if job == nil {
		return
	}

	if job.Enabled == 0 || job.Stat == models.STATE_REALLOC { //任务状态已关闭或未分配
		logger.INFO("[#cache#] job %s set action to %s return, job enable is %d, state %d", jobid, action, job.Enabled, job.Stat)
		return
	}

	if action == "start" && job.Stat == models.STATE_STARTED { //任务已在运行中
		logger.INFO("[#cache#] job %s set action to %s return, job is started, state %d", jobid, action, job.Stat)
		return
	}

	jobData := cacheRepository.GetAllocJob(location, jobid)
	if jobData != nil {
		if worker := cacheRepository.GetWorker(jobData.Key); worker != nil {
			if err := putJobAction(cacheRepository.client, worker.APIAddr, location, jobid, action); err != nil {
				logger.ERROR("[#cache#] set worker %s job action failure, %s", worker.IPAddr, err)
				return
			}
			logger.INFO("[#cache#] set worker %s job %s action to %s", worker.IPAddr, jobid, action)
		}
	}
}

//SetJobState is exported
func (cacheRepository *CacheRepository) SetJobState(jobid string, state int) {

	logger.INFO("[#cache#] set job state, %s %d", jobid, state)
	job := cacheRepository.GetJob(jobid)
	if job != nil {
		job.Stat = state
		if state == models.STATE_STARTED || state == models.STATE_REALLOC {
			job.NextAt = time.Time{}
		}
		logger.INFO("[#cache#] set job %s state to %d, nextat %s, execat %s", jobid, job.Stat, job.NextAt, job.ExecAt)
		cacheRepository.storageDriver.SetJob(job)
	}
}

//SetJobNextAt is exported
func (cacheRepository *CacheRepository) SetJobNextAt(jobid string, nextat time.Time) {

	logger.INFO("[#cache#] set job nextat, %s %s", jobid, nextat)
	job := cacheRepository.GetJob(jobid)
	if job != nil {
		if len(job.Schedule) > 0 {
			job.Stat = models.STATE_STOPED
			job.NextAt = nextat
			logger.INFO("[#cache#] set job %s nextat to %s, execat %s, state %d", jobid, job.NextAt, job.ExecAt, job.Stat)
			cacheRepository.storageDriver.SetJob(job)
		}
	}
}

//SetJobExecute is exported
func (cacheRepository *CacheRepository) SetJobExecute(jobid string, state int, execerr string, execat time.Time, nextat time.Time) {

	logger.INFO("[#cache#] set job execute, %s %d execat:%s nextat:%s", jobid, state, execat, nextat)
	job := cacheRepository.GetJob(jobid)
	if job != nil {
		job.Stat = state
		job.ExecAt = execat
		if state == models.STATE_STARTED {
			job.NextAt = time.Time{}
			if job.Enabled == 0 {
				job.Stat = models.STATE_REALLOC
			}
		} else if state == models.STATE_STOPED || state == models.STATE_FAILED {
			job.ExecErr = execerr
			job.NextAt = nextat
		}
		logger.INFO("[#cache#] set job %s execute to %d, nextat %s, execat %s", jobid, job.Stat, job.NextAt, job.ExecAt)
		cacheRepository.storageDriver.SetJob(job)
	}
}

//SetJobLog is exported
func (cacheRepository *CacheRepository) SetJobLog(joblog *models.JobLog) {

	logger.INFO("[#cache#] set job log, %s %d", joblog.JobId, joblog.Stat)
	cacheRepository.storageDriver.SetJobLog(joblog)
}

//GetWorker is exported
func (cacheRepository *CacheRepository) GetWorker(key string) *Worker {

	return cacheRepository.nodeStore.GetWorker(key)
}

//GetWorkers is exported
func (cacheRepository *CacheRepository) GetWorkers(location string) []*Worker {

	return cacheRepository.nodeStore.GetWorkers(location)
}

//ChoiceWorker is exported
func (cacheRepository *CacheRepository) ChoiceWorker(location string, key string, servers []string) *Worker {

	if len(servers) > 0 {
		return cacheRepository.nodeStore.HashLocationRangeWorker(location, key, servers)
	}
	return cacheRepository.nodeStore.HashLocationWorker(location, key)
}

//CreateWorker is exported
func (cacheRepository *CacheRepository) CreateWorker(key string, node *gzkwrapper.NodeData) {

	worker := cacheRepository.nodeStore.GetWorker(key)
	if worker != nil {
		logger.WARN("[#cache#] %s create worker %s key is already, %s(%s).", node.Location, node.HostName, node.IpAddr)
		return
	}

	ret := cacheRepository.nodeStore.ContainsLocationWorker(node.Location, node.IpAddr, node.HostName)
	if ret {
		logger.WARN("[#cache#] %s create worker %s ip or host is already, %s(%s).", node.Location, node.HostName, node.IpAddr)
		return
	}

	ret = cacheRepository.localStorage.ContainsLocationServer(node.Location, node.IpAddr, node.HostName)
	if ret {
		server := models.CreateServer(key, node, 1)
		attach := models.AttachDecode(node.Attach)
		cacheRepository.nodeStore.CreateWorker(node.Location, attach, server)
		logger.INFO("[#cache#] %s create worker %s, %s(%s).", node.Location, key, node.HostName, node.IpAddr)
	}
}

//ChangeWorker is exported
func (cacheRepository *CacheRepository) ChangeWorker(key string, node *gzkwrapper.NodeData) {

	worker := cacheRepository.nodeStore.GetWorker(key)
	if worker == nil {
		logger.WARN("[#cache#] %s change worker %s key is not found, %s(%s).", node.Location, node.HostName, node.IpAddr)
		return
	}

	ret := cacheRepository.localStorage.ContainsLocationServer(node.Location, node.IpAddr, node.HostName)
	if ret {
		server := models.CreateServer(key, node, 1)
		attach := models.AttachDecode(node.Attach)
		cacheRepository.nodeStore.ChangeWorker(node.Location, attach, server)
		logger.INFO("[#cache#] %s change worker %s, %s(%s).", node.Location, key, node.HostName, node.IpAddr)
	}
}

//RemoveWorker is exported
func (cacheRepository *CacheRepository) RemoveWorker(key string, node *gzkwrapper.NodeData) {

	worker := cacheRepository.nodeStore.GetWorker(key)
	if worker != nil && worker.Alivestamp == node.Alivestamp {
		cacheRepository.nodeStore.RemoveWorker(node.Location, key)
		logger.INFO("[#cache#] %s remove worker %s, %s(%s).", node.Location, key, node.HostName, node.IpAddr)
	}
}
