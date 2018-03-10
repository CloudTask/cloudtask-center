package cache

import "github.com/cloudtask/common/models"
import lru "github.com/hashicorp/golang-lru"

import (
	"strings"
	"sync"
)

const (
	MinLRUSize     = 128
	MaxLRUSize     = 1024
	DefaultLRUSize = 256
)

//LocalStorage is exported
type LocalStorage struct {
	sync.RWMutex
	lurSize       int
	jobsCache     *lru.Cache
	workLocations map[string]*models.WorkLocation
}

//NewLocalStorage is exported
func NewLocalStorage(lurSize int) *LocalStorage {

	if lurSize < MinLRUSize || lurSize > MaxLRUSize {
		lurSize = DefaultLRUSize
	}

	jobsCache, _ := lru.New(lurSize)
	return &LocalStorage{
		lurSize:       lurSize,
		jobsCache:     jobsCache,
		workLocations: make(map[string]*models.WorkLocation),
	}
}

//Clear is exported
func (storage *LocalStorage) Clear() {

	storage.Lock()
	storage.jobsCache, _ = lru.New(storage.lurSize)
	storage.workLocations = make(map[string]*models.WorkLocation)
	storage.Unlock()
}

//ContainsLocationServer is exported
func (storage *LocalStorage) ContainsLocationServer(location string, ipaddr string, hostname string) bool {

	servers := storage.GetLocationServers(location)
	for _, server := range servers {
		if server.IPAddr == ipaddr || strings.ToUpper(server.Name) == strings.ToUpper(hostname) {
			return true
		}
	}
	return false
}

//GetLocation is exported
func (storage *LocalStorage) GetLocation(location string) *models.WorkLocation {

	storage.RLock()
	defer storage.RUnlock()
	workLocation, ret := storage.workLocations[location]
	if ret {
		return workLocation
	}
	return nil
}

//GetLocationsName is exported
func (storage *LocalStorage) GetLocationsName() []string {

	names := []string{}
	storage.RLock()
	for location := range storage.workLocations {
		names = append(names, location)
	}
	storage.RUnlock()
	return names
}

//GetLocationServers is exported
func (storage *LocalStorage) GetLocationServers(location string) []*models.Server {

	servers := []*models.Server{}
	workLocation := storage.GetLocation(location)
	if workLocation != nil {
		servers = workLocation.Server
	}
	return servers
}

//GetLocationGroups is exported
func (storage *LocalStorage) GetLocationGroups(location string) []*models.Group {

	groups := []*models.Group{}
	workLocation := storage.GetLocation(location)
	if workLocation != nil {
		groups = workLocation.Group
	}
	return groups
}

//GetLocationGroup is exported
func (storage *LocalStorage) GetLocationGroup(location string, groupid string) *models.Group {

	workLocation := storage.GetLocation(location)
	if workLocation != nil {
		for _, group := range workLocation.Group {
			if group.Id == groupid {
				return group
			}
		}
	}
	return nil
}

//SetLocation is exported
func (storage *LocalStorage) SetLocation(workLocation *models.WorkLocation) {

	if workLocation != nil {
		storage.Lock()
		storage.workLocations[workLocation.Location] = workLocation
		storage.Unlock()
	}
}

//RemoveLocation is exported
func (storage *LocalStorage) RemoveLocation(location string) {

	storage.Lock()
	delete(storage.workLocations, location)
	storage.Unlock()
}

//GetJob is exported
func (storage *LocalStorage) GetJob(jobid string) *models.Job {

	value, ret := storage.jobsCache.Get(jobid)
	if ret {
		return value.(*models.Job)
	}
	return nil
}

//AddJob is exported
func (storage *LocalStorage) AddJob(job *models.Job) {

	if job != nil {
		storage.jobsCache.Add(job.JobId, job)
	}
}

//RemoveJob is exported
func (storage *LocalStorage) RemoveJob(jobid string) {

	storage.jobsCache.Remove(jobid)
}
