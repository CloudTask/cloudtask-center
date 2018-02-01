package cache

import _ "github.com/cloudtask/cloudtask-center/cache/driver/mongo"
import _ "github.com/cloudtask/cloudtask-center/cache/driver/ngcloud"
import "github.com/cloudtask/cloudtask-center/cache/driver"
import "github.com/cloudtask/cloudtask-center/cache/driver/types"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/logger"
import "github.com/cloudtask/libtools/gzkwrapper"
import "github.com/cloudtask/libtools/gounits/httpx"
import lru "github.com/hashicorp/golang-lru"

import (
	"net"
	"net/http"
	"time"
)

const (
	defaultLRUSize = 512
)

//CacheConfigs is exported
type CacheConfigs struct {
	LRUSize           int
	StorageBackend    types.Backend
	StorageParameters types.Parameters
}

//CacheRepository is expotred
type CacheRepository struct {
	jobsCache     *lru.Cache
	nodeStore     *NodeStore
	allocStore    *AllocStore
	client        *httpx.HttpClient
	storageDriver driver.StorageDriver
}

//NewCacheRepository is expotred
func NewCacheRepository(configs *CacheConfigs, handler ICacheRepositoryHandler) (*CacheRepository, error) {

	storageDriver, err := driver.NewDriver(configs.StorageBackend, configs.StorageParameters)
	if err != nil {
		return nil, err
	}

	if configs.LRUSize <= 0 {
		configs.LRUSize = defaultLRUSize
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

	jobsCache, _ := lru.New(configs.LRUSize)
	return &CacheRepository{
		jobsCache:     jobsCache,
		nodeStore:     NewNodeStore(handler),
		allocStore:    NewAllocStore(handler),
		client:        client,
		storageDriver: storageDriver,
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

//Clear is expotred
func (cacheRepository *CacheRepository) Clear() {

	cacheRepository.nodeStore.ClearWorkers()
	cacheRepository.allocStore.ClearAlloc()
}

//DumpCleanAlloc is exported
func (cacheRepository *CacheRepository) DumpCleanAlloc(location string) {

	jobsAlloc := cacheRepository.GetAlloc(location)
	if jobsAlloc == nil {
		return
	}

	var (
		found  = false
		jobIds = []string{}
	)

	//将分配表存在而数据库不存在或关闭的任务从分配表删除
	jobs := cacheRepository.GetLocationJobs(location)
	for _, jobData := range jobsAlloc.Jobs {
		for _, job := range jobs {
			if job.JobId == jobData.JobId && job.Enabled == 1 {
				found = true
				break
			}
		}
		if !found {
			cacheRepository.SetJobState(jobData.JobId, models.STATE_REALLOC)
			jobIds = append(jobIds, jobData.JobId)
		}
		found = false
	}

	if len(jobIds) > 0 {
		cacheRepository.RemoveAllocJobs(location, jobIds)
	}
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
		}
		if job := cacheRepository.GetSimpleJob(jobData.JobId); job != nil {
			jobDataInfo.JobName = job.Name
		}
		jobsAllocData.Data = append(jobsAllocData.Data, jobDataInfo)
	}
	return jobsAllocData
}

//MakeAllocBuffer is exported
func (cacheRepository *CacheRepository) MakeAllocBuffer() ([]byte, error) {

	return cacheRepository.allocStore.MakeAllocBuffer()
}

//ClearAllocBuffer is exported
func (cacheRepository *CacheRepository) ClearAllocBuffer(location string) {

	cacheRepository.allocStore.ClearAllocBuffer(location)
}

//InitAllocBuffer is exported
func (cacheRepository *CacheRepository) InitAllocBuffer(location string, data []byte) {

	jobsAlloc := cacheRepository.GetAlloc(location)
	if jobsAlloc == nil {
		if data == nil { //new make alloc buffer
			data, _ = cacheRepository.MakeAllocBuffer()
		}
		//set alloc buffer to local.
		cacheRepository.SetAllocBuffer(location, data)
		//clear location alloc. wait node online begin re-alloc.
		cacheRepository.ClearAllocBuffer(location)
	}
}

//SetAllocBuffer is exported
func (cacheRepository *CacheRepository) SetAllocBuffer(location string, data []byte) error {

	return cacheRepository.allocStore.SetAllocBuffer(location, data)
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

	return cacheRepository.storageDriver.GetLocationsName()
}

//GetLocationGroups is exported
func (cacheRepository *CacheRepository) GetLocationGroups(location string) []*models.Group {

	groups := []*models.Group{}
	workLocation := cacheRepository.storageDriver.GetLocation(location)
	if workLocation != nil {
		groups = workLocation.Group
	}
	return groups
}

//GetLocationGroup is exported
func (cacheRepository *CacheRepository) GetLocationGroup(location string, groupid string) *models.Group {

	workLocation := cacheRepository.storageDriver.GetLocation(location)
	if workLocation != nil {
		for _, group := range workLocation.Group {
			if group.Id == groupid {
				return group
			}
		}
	}
	return nil
}

//GetLocationSimpleJobs is exported
func (cacheRepository *CacheRepository) GetLocationSimpleJobs(location string) []*models.SimpleJob {

	return cacheRepository.storageDriver.GetLocationSimpleJobs(location)
}

//GetSimpleJob is exported
func (cacheRepository *CacheRepository) GetSimpleJob(jobid string) *models.SimpleJob {

	return cacheRepository.storageDriver.GetSimpleJob(jobid)
}

//GetJobs is exported
func (cacheRepository *CacheRepository) GetJobs() []*models.Job {

	jobs := cacheRepository.storageDriver.GetJobs()
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetStateJobs is exported
func (cacheRepository *CacheRepository) GetStateJobs(state int) []*models.Job {

	jobs := cacheRepository.storageDriver.GetStateJobs(state)
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetLocationJobs is exported
func (cacheRepository *CacheRepository) GetLocationJobs(location string) []*models.Job {

	jobs := cacheRepository.storageDriver.GetLocationJobs(location)
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetGroupJobs is exported
func (cacheRepository *CacheRepository) GetGroupJobs(groupid string) []*models.Job {

	jobs := cacheRepository.storageDriver.GetGroupJobs(groupid)
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetRawJob is exported
func (cacheRepository *CacheRepository) GetRawJob(jobid string) *models.Job {

	job := cacheRepository.storageDriver.GetJob(jobid)
	if job != nil {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return job
}

//GetJob is exported
func (cacheRepository *CacheRepository) GetJob(jobid string) *models.Job {

	job, ret := cacheRepository.jobsCache.Get(jobid)
	if !ret {
		value := cacheRepository.storageDriver.GetJob(jobid)
		if value == nil {
			return nil
		}
		job = value
		cacheRepository.jobsCache.Add(value.JobId, value)
	}
	return job.(*models.Job)
}

//RemoveJob is exported
func (cacheRepository *CacheRepository) RemoveJob(jobid string) {

	cacheRepository.jobsCache.Remove(jobid)
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

//SetJobLog is exported
func (cacheRepository *CacheRepository) SetJobLog(joblog *models.JobLog) {

	logger.INFO("[#cache#] set job log, %s %d", joblog.JobId, joblog.Stat)
	cacheRepository.storageDriver.SetJobLog(joblog)
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

	server := cacheRepository.storageDriver.CreateLocationServer(node.Location, key, node.HostName, node.IpAddr, node.APIAddr, node.OS, node.Platform)
	if server != nil {
		attach := models.AttachDecode(node.Attach)
		cacheRepository.nodeStore.CreateWorker(node.Location, attach, server)
	}
}

//ChangeWorker is exported
func (cacheRepository *CacheRepository) ChangeWorker(key string, node *gzkwrapper.NodeData) {

	server := cacheRepository.storageDriver.ChangeLocationServer(node.Location, key, node.HostName, node.IpAddr, node.APIAddr, node.OS, node.Platform)
	if server != nil {
		attach := models.AttachDecode(node.Attach)
		cacheRepository.nodeStore.ChangeWorker(node.Location, attach, server)
	}
}

//RemoveWorker is exported
func (cacheRepository *CacheRepository) RemoveWorker(key string, node *gzkwrapper.NodeData) {

	cacheRepository.nodeStore.RemoveWorker(node.Location, key)
	cacheRepository.storageDriver.RemoveLocationServer(node.Location, key)
}

//RemoveLocation is exported
func (cacheRepository *CacheRepository) RemoveLocation(location string) {

	cacheRepository.allocStore.RemoveAlloc(location)
	cacheRepository.nodeStore.RemoveLocation(location)
}
