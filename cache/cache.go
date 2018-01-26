package cache

import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gzkwrapper"
import "github.com/cloudtask/libtools/gounits/logger"
import lru "github.com/hashicorp/golang-lru"

import (
	"strconv"
	"time"
)

const (
	DefaultLRUSize = 256
)

//CacheRepositoryArgs is exported
type CacheRepositoryArgs struct {
	LRUSize       int
	CloudPageSize int
}

//CacheRepository is expotred
type CacheRepository struct {
	jobsCache       *lru.Cache
	nodeStore       *NodeStore
	allocStore      *AllocStore
	cloudDataClient *CloudDataClient
	serverConfig    *models.ServerConfig
}

//NewCacheRepository is expotred
func NewCacheRepository(args *CacheRepositoryArgs, serverConfig *models.ServerConfig, handler ICacheRepositoryHandler) *CacheRepository {

	if args.LRUSize <= 0 {
		args.LRUSize = DefaultLRUSize
	}

	jobsCache, _ := lru.New(args.LRUSize)
	return &CacheRepository{
		jobsCache:       jobsCache,
		nodeStore:       NewNodeStore(handler),
		allocStore:      NewAllocStore(handler),
		cloudDataClient: NewCloudDataClient(args.CloudPageSize, serverConfig),
		serverConfig:    serverConfig,
	}
}

//Clear is expotred
func (cacheRepository *CacheRepository) Clear() {

	cacheRepository.nodeStore.ClearWorkers()
	cacheRepository.allocStore.ClearAlloc()
}

//SetServerConfig is exported
//setting serverConfig
func (cacheRepository *CacheRepository) SetServerConfig(serverConfig *models.ServerConfig) {

	cacheRepository.serverConfig = serverConfig
	cacheRepository.cloudDataClient.SetServerConfig(serverConfig)
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

	return cacheRepository.cloudDataClient.GetLocationsName()
}

//GetLocationGroups is exported
func (cacheRepository *CacheRepository) GetLocationGroups(location string) []*models.Group {

	groups := []*models.Group{}
	workLocation := cacheRepository.cloudDataClient.GetLocation(location)
	if workLocation != nil {
		groups = workLocation.Group
	}
	return groups
}

//GetLocationGroup is exported
func (cacheRepository *CacheRepository) GetLocationGroup(location string, groupid string) *models.Group {

	workLocation := cacheRepository.cloudDataClient.GetLocation(location)
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

	query := map[string][]string{"f_location": []string{location}}
	return cacheRepository.cloudDataClient.GetSimpleJobs(query)
}

//GetSimpleJob is exported
func (cacheRepository *CacheRepository) GetSimpleJob(jobid string) *models.SimpleJob {

	return cacheRepository.cloudDataClient.GetSimpleJob(jobid)
}

//GetJobs is exported
func (cacheRepository *CacheRepository) GetJobs() []*models.Job {

	query := map[string][]string{}
	jobs := cacheRepository.cloudDataClient.GetJobs(query)
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetStateJobs is exported
func (cacheRepository *CacheRepository) GetStateJobs(state int) []*models.Job {

	query := map[string][]string{"f_stat": []string{strconv.Itoa(state)}}
	jobs := cacheRepository.cloudDataClient.GetJobs(query)
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetLocationJobs is exported
func (cacheRepository *CacheRepository) GetLocationJobs(location string) []*models.Job {

	query := map[string][]string{"f_location": []string{location}}
	jobs := cacheRepository.cloudDataClient.GetJobs(query)
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetGroupJobs is exported
func (cacheRepository *CacheRepository) GetGroupJobs(groupid string) []*models.Job {

	query := map[string][]string{"f_groupid": []string{groupid}}
	jobs := cacheRepository.cloudDataClient.GetJobs(query)
	for _, job := range jobs {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return jobs
}

//GetRawJob is exported
func (cacheRepository *CacheRepository) GetRawJob(jobid string) *models.Job {

	job := cacheRepository.cloudDataClient.GetJob(jobid)
	if job != nil {
		cacheRepository.jobsCache.Add(job.JobId, job)
	}
	return job
}

//GetJob is exported
func (cacheRepository *CacheRepository) GetJob(jobid string) *models.Job {

	job, ret := cacheRepository.jobsCache.Get(jobid)
	if !ret {
		value := cacheRepository.cloudDataClient.GetJob(jobid)
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
			logger.INFO("[#cache#] clouddata set job %s action to %s, worker is %s(%s)", jobid, action, worker.Key, worker.IPAddr)
			cacheRepository.cloudDataClient.SetJobAction(worker.APIAddr, location, jobid, action)
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
			logger.INFO("[#cache#] clouddata set job %s nextat to %s, execat %s, state %d", jobid, job.NextAt, job.ExecAt, job.Stat)
			cacheRepository.cloudDataClient.SetJob(job)
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
		logger.INFO("[#cache#] clouddata set job %s state to %d, nextat %s, execat %s", jobid, job.Stat, job.NextAt, job.ExecAt)
		cacheRepository.cloudDataClient.SetJob(job)
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
		logger.INFO("[#cache#] clouddata set job %s execute to %d, nextat %s, execat %s", jobid, job.Stat, job.NextAt, job.ExecAt)
		cacheRepository.cloudDataClient.SetJob(job)
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

	server := cacheRepository.cloudDataClient.CreateLocationServer(node.DataCenter, node.Location, key, node.HostName, node.IpAddr, node.APIAddr, node.OS, node.Platform)
	if server != nil {
		attach := models.AttachDecode(node.Attach)
		cacheRepository.nodeStore.CreateWorker(node.Location, attach, server)
	}
}

//ChangeWorker is exported
func (cacheRepository *CacheRepository) ChangeWorker(key string, node *gzkwrapper.NodeData) {

	server := cacheRepository.cloudDataClient.ChangeLocationServer(node.Location, key, node.HostName, node.IpAddr, node.APIAddr, node.OS, node.Platform)
	if server != nil {
		attach := models.AttachDecode(node.Attach)
		cacheRepository.nodeStore.ChangeWorker(node.Location, attach, server)
	}
}

//RemoveWorker is exported
func (cacheRepository *CacheRepository) RemoveWorker(key string, node *gzkwrapper.NodeData) {

	cacheRepository.nodeStore.RemoveWorker(node.Location, key)
	cacheRepository.cloudDataClient.RemoveLocationServer(node.Location, key)
}

//RemoveLocation is exported
func (cacheRepository *CacheRepository) RemoveLocation(location string) {

	cacheRepository.allocStore.RemoveAlloc(location)
	cacheRepository.nodeStore.RemoveLocation(location)
}
