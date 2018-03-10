package cache

import "github.com/cloudtask/common/models"

import (
	"bytes"
	"sync"
)

//AllocEvent is exported
type AllocEvent int

const (
	ALLOC_CREATED_EVENT AllocEvent = iota + 1
	ALLOC_CHANGED_EVENT
	ALLOC_REMOVED_EVENT
)

func (event AllocEvent) String() string {

	switch event {
	case ALLOC_CREATED_EVENT:
		return "ALLOC_CREATED_EVENT"
	case ALLOC_CHANGED_EVENT:
		return "ALLOC_CHANGED_EVENT"
	case ALLOC_REMOVED_EVENT:
		return "ALLOC_REMOVED_EVENT"
	}
	return ""
}

//AllocCacheEventHandlerFunc is exported
type AllocCacheEventHandlerFunc func(event AllocEvent, location string, data []byte, err error)

//AllocStore is exported
type AllocStore struct {
	sync.RWMutex
	allocPool *sync.Pool
	tableData models.AllocMapper
	callback  AllocCacheEventHandlerFunc
}

//NewAllocStore is exported
func NewAllocStore(callback AllocCacheEventHandlerFunc) *AllocStore {

	allocPool := &sync.Pool{
		New: func() interface{} { //数据编码池,默认分配128K
			return bytes.NewBuffer(make([]byte, 0, 128<<10))
		},
	}

	return &AllocStore{
		allocPool: allocPool,
		tableData: make(models.AllocMapper, 0),
		callback:  callback,
	}
}

//ClearAlloc is exported
func (store *AllocStore) ClearAlloc() {

	store.Lock()
	defer store.Unlock()
	for location := range store.tableData {
		store.tableData[location].Jobs = []*models.JobData{}
		delete(store.tableData, location)
	}
}

//GetAlloc is exported
//return location jobsalloc.
func (store *AllocStore) GetAlloc(location string) *models.JobsAlloc {

	store.RLock()
	defer store.RUnlock()
	if jobsAlloc, ret := store.tableData[location]; ret {
		return jobsAlloc
	}
	return nil
}

//HasAlloc is exported
func (store *AllocStore) HasAlloc(location string) bool {

	store.RLock()
	defer store.RUnlock()
	_, ret := store.tableData[location]
	return ret
}

//HasAllocJobId is exported
func (store *AllocStore) HasAllocJobId(location string, jobId string) bool {

	store.RLock()
	defer store.RUnlock()
	jobsAlloc, ret := store.tableData[location]
	if !ret {
		return false
	}

	for _, jobData := range jobsAlloc.Jobs {
		if jobData.JobId == jobId {
			return true
		}
	}
	return false
}

//GetAllocJobIds is exported
//return all jobs ids contained in location key.
func (store *AllocStore) GetAllocJobIds(location string, key string) []string {

	store.RLock()
	defer store.RUnlock()
	jobIds := []string{}
	if jobsAlloc, ret := store.tableData[location]; ret {
		for _, jobData := range jobsAlloc.Jobs {
			if jobData.Key == key {
				jobIds = append(jobIds, jobData.JobId)
			}
		}
	}
	return jobIds
}

//SetAllocJobsKey is exported
//set location alloc's jobs key.
func (store *AllocStore) SetAllocJobsKey(location string, jobs map[string]string) {

	var (
		ret       bool
		jobsAlloc *models.JobsAlloc
	)

	store.Lock()
	defer store.Unlock()
	if jobsAlloc, ret = store.tableData[location]; !ret {
		return
	}

	for jobid, key := range jobs {
		for _, jobData := range jobsAlloc.Jobs {
			if jobData.JobId == jobid {
				jobData.Key = key
				break
			}
		}
	}

	jobsAlloc.Version = jobsAlloc.Version + 1
	data, err := models.JobsAllocEnCode(store.allocPool, jobsAlloc)
	store.callback(ALLOC_CHANGED_EVENT, location, data, err)
}

//GetAllocJob is exported
//return location alloc's jobdata.
func (store *AllocStore) GetAllocJob(location string, jobId string) *models.JobData {

	store.RLock()
	defer store.RUnlock()
	if jobsAlloc, ret := store.tableData[location]; ret {
		for _, jobData := range jobsAlloc.Jobs {
			if jobData.JobId == jobId {
				return jobData
			}
		}
	}
	return nil
}

//CreateAllocJob is exported
//create jobdata to location alloc.
func (store *AllocStore) CreateAllocJob(location string, key string, jobId string) {

	store.Lock()
	defer store.Unlock()
	var allocEvent AllocEvent
	jobsAlloc, ret := store.tableData[location]
	if !ret {
		jobsAlloc = &models.JobsAlloc{
			Version: 1,
			Jobs: []*models.JobData{
				&models.JobData{
					JobId:   jobId,
					Key:     key,
					Version: 1,
				},
			},
		}
		store.tableData[location] = jobsAlloc
		allocEvent = ALLOC_CREATED_EVENT
	} else {
		found := false
		for _, jobData := range jobsAlloc.Jobs {
			if jobData.JobId == jobId {
				jobData.Version = jobData.Version + 1
				found = true
				break
			}
		}
		if !found {
			jobsAlloc.Jobs = append(jobsAlloc.Jobs, &models.JobData{
				JobId:   jobId,
				Key:     key,
				Version: 1})
		}
		jobsAlloc.Version = jobsAlloc.Version + 1
		allocEvent = ALLOC_CHANGED_EVENT
	}

	data, err := models.JobsAllocEnCode(store.allocPool, jobsAlloc)
	store.callback(allocEvent, location, data, err)
}

//UpdateAllocJobs is exported
//update location's jobIds version & alloc version.
func (store *AllocStore) UpdateAllocJobs(location string, jobIds []string) {

	var (
		ret       bool
		jobsAlloc *models.JobsAlloc
	)

	store.Lock()
	defer store.Unlock()
	if jobsAlloc, ret = store.tableData[location]; !ret {
		return
	}

	for _, jobId := range jobIds {
		for _, jobData := range jobsAlloc.Jobs {
			if jobData.JobId == jobId {
				jobData.Version = jobData.Version + 1
				break
			}
		}
	}

	jobsAlloc.Version = jobsAlloc.Version + 1
	data, err := models.JobsAllocEnCode(store.allocPool, jobsAlloc)
	store.callback(ALLOC_CHANGED_EVENT, location, data, err)
}

//RemoveAllocJob is exported
//remove jobdata to location alloc.
func (store *AllocStore) RemoveAllocJob(location string, jobId string) {

	var (
		ret       bool
		jobsAlloc *models.JobsAlloc
	)

	store.Lock()
	defer store.Unlock()
	if jobsAlloc, ret = store.tableData[location]; !ret {
		return
	}

	for i, jobData := range jobsAlloc.Jobs {
		if jobData.JobId == jobId {
			jobsAlloc.Jobs = append(jobsAlloc.Jobs[:i], jobsAlloc.Jobs[i+1:]...)
			jobsAlloc.Version = jobsAlloc.Version + 1
			data, err := models.JobsAllocEnCode(store.allocPool, jobsAlloc)
			store.callback(ALLOC_CHANGED_EVENT, location, data, err)
			break
		}
	}
}

//RemoveAllocJobs is exported
func (store *AllocStore) RemoveAllocJobs(location string, jobIds []string) {

	var (
		ret       bool
		jobsAlloc *models.JobsAlloc
	)

	store.Lock()
	defer store.Unlock()
	if jobsAlloc, ret = store.tableData[location]; !ret {
		return
	}

	found := false
	for _, jobId := range jobIds {
		for i, jobData := range jobsAlloc.Jobs {
			if jobData.JobId == jobId {
				jobsAlloc.Jobs = append(jobsAlloc.Jobs[:i], jobsAlloc.Jobs[i+1:]...)
				found = true
				break
			}
		}
	}

	if found {
		jobsAlloc.Version = jobsAlloc.Version + 1
		data, err := models.JobsAllocEnCode(store.allocPool, jobsAlloc)
		store.callback(ALLOC_CHANGED_EVENT, location, data, err)
	}
}

//CreateAlloc is exported
func (store *AllocStore) CreateAlloc(location string, data []byte) {

	store.Lock()
	if _, ret := store.tableData[location]; !ret {
		jobsAlloc := &models.JobsAlloc{}
		var err error
		if err = models.JobsAllocDeCode(data, jobsAlloc); err == nil {
			store.tableData[location] = jobsAlloc
		}
		store.callback(ALLOC_CREATED_EVENT, location, data, err)
	}
	store.Unlock()
}

//RemoveAlloc is exported
func (store *AllocStore) RemoveAlloc(location string) {

	store.Lock()
	if jobsAlloc, ret := store.tableData[location]; ret {
		delete(store.tableData, location)
		jobsAlloc.Jobs = []*models.JobData{}
		jobsAlloc.Version = jobsAlloc.Version + 1
		data, err := models.JobsAllocEnCode(store.allocPool, jobsAlloc)
		store.callback(ALLOC_REMOVED_EVENT, location, data, err)
	}
	store.Unlock()
}
