package mongo

import "github.com/cloudtask/cloudtask-center/cache/driver"
import "github.com/cloudtask/cloudtask-center/cache/driver/types"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/logger"

import (
	"errors"
	"net/url"
	"path"
	"strconv"
	"sync"
)

const (
	defaultMaxPoolSize = 20
)

var (
	ErrMongoStorageDriverHostsInvalid    = errors.New("mongo storage driver hosts invalid.")
	ErrMongoStorageDriverDataBaseInvalid = errors.New("mongo storage driver database invalid.")
)

//MongoStorageDriver is exported
type MongoStorageDriver struct {
	sync.RWMutex
	driver.StorageDriver
	engine *Engine
}

func init() {
	driver.AddDriver(types.MONGO, New)
}

//New is exported
func New(parameters types.Parameters) (driver.StorageDriver, error) {

	var (
		value       interface{}
		ret         bool
		rawHosts    string
		dbName      string
		user        string
		password    string
		maxPoolSize int
	)

	value, ret = parameters["hosts"]
	if !ret {
		return nil, ErrMongoStorageDriverHostsInvalid
	}

	pHosts, err := url.Parse(value.(string))
	if err != nil {
		return nil, ErrMongoStorageDriverHostsInvalid
	}

	scheme := pHosts.Scheme
	if scheme == "" {
		scheme = "mongodb"
	}

	maxPoolSize = defaultMaxPoolSize
	queryPoolSize := pHosts.Query().Get("maxPoolSize")
	if queryPoolSize != "" {
		if pValue, err := strconv.Atoi(queryPoolSize); err == nil {
			maxPoolSize = pValue
		}
	}

	value, ret = parameters["database"]
	if !ret {
		return nil, ErrMongoStorageDriverDataBaseInvalid
	}

	if dbName = value.(string); dbName == "" {
		return nil, ErrMongoStorageDriverDataBaseInvalid
	}

	if value, ret = parameters["user"]; ret {
		user = value.(string)
	}

	if value, ret = parameters["password"]; ret {
		password = value.(string)
	}

	rawHosts = scheme + "://" + pHosts.Host + path.Clean(pHosts.Path) + "?" + pHosts.RawQuery
	return &MongoStorageDriver{
		engine: NewEngine(rawHosts, dbName, maxPoolSize, user, password),
	}, nil
}

//Open is exported
func (driver *MongoStorageDriver) Open() error {

	return driver.engine.Open()
}

//Close is exported
func (driver *MongoStorageDriver) Close() {

	driver.engine.Close()
}

//GetLocationsName is exported
func (driver *MongoStorageDriver) GetLocationsName() []string {

	driver.RLock()
	defer driver.RUnlock()
	names, err := driver.engine.readLocationsName()
	if err != nil {
		logger.ERROR("[#cache#] engine read locations name error, %s", err.Error())
		return []string{}
	}
	return names
}

//GetLocation is exported
func (driver *MongoStorageDriver) GetLocation(location string) *models.WorkLocation {

	driver.RLock()
	defer driver.RUnlock()
	workLocation, err := driver.engine.getLocation(location)
	if err != nil {
		logger.ERROR("[#cache#] engine read location %s error, %s", location, err.Error())
		return nil
	}
	return workLocation
}

//GetLocationServer is exported
func (driver *MongoStorageDriver) GetLocationServer(location string, key string) *models.Server {

	driver.RLock()
	defer driver.RUnlock()
	workLocation, err := driver.engine.getLocation(location)
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
func (driver *MongoStorageDriver) GetLocationServers(location string) []*models.Server {

	driver.RLock()
	defer driver.RUnlock()
	workLocation, err := driver.engine.getLocation(location)
	if err != nil {
		logger.ERROR("[#cache#] engine read location %s servers error, %s", location, err.Error())
		return []*models.Server{}
	}
	return workLocation.Server
}

//CreateLocationServer is exported
func (driver *MongoStorageDriver) CreateLocationServer(location string, key string, hostname string,
	ipaddr string, apiaddr string, os string, platform string) *models.Server {

	driver.Lock()
	defer driver.Unlock()
	workLocation, err := driver.engine.getLocation(location)
	if err != nil {
		if err != types.ErrDriverResourceNotFound {
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

	if err == types.ErrDriverResourceNotFound {
		workLocation = &models.WorkLocation{
			Location: location,
			Group:    []*models.Group{},
			Server:   []*models.Server{server},
		}
		if err = driver.engine.postLocation(workLocation); err != nil {
			logger.ERROR("[#cache#] engine create server %s write location %s error, %s", key, location, err.Error())
			return nil
		}
		return server
	}

	found := false
	for _, s := range workLocation.Server {
		if s.Key == server.Key {
			server = s //若存在，则返回已经存在的server对象，不替换.
			found = true
			break
		}
	}

	if !found {
		workLocation.Server = append(workLocation.Server, server)
		if err = driver.engine.putLocation(workLocation); err != nil {
			logger.ERROR("[#cache#] engine create server %s write location %s error, %s", key, location, err.Error())
			return nil
		}
	}
	return server
}

//ChangeLocationServer is exported
func (driver *MongoStorageDriver) ChangeLocationServer(location string, key string, hostname string,
	ipaddr string, apiaddr string, os string, platform string) *models.Server {

	driver.Lock()
	defer driver.Unlock()
	workLocation, err := driver.engine.getLocation(location)
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
		err = driver.engine.putLocation(workLocation)
		if err != nil {
			logger.ERROR("[#cache#] engine change server %s write location %s error, %s", key, location, err.Error())
			return nil
		}
	}
	return server
}

//RemoveLocationServer is exported
func (driver *MongoStorageDriver) RemoveLocationServer(location string, key string) {

	driver.Lock()
	defer driver.Unlock()
	workLocation, err := driver.engine.getLocation(location)
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
		err = driver.engine.putLocation(workLocation)
		if err != nil {
			logger.ERROR("[#cache#] engine remove server %s write location %s error, %s", key, location, err.Error())
		}
	}
}

//GetLocationSimpleJobs is exported
func (driver *MongoStorageDriver) GetLocationSimpleJobs(location string) []*models.SimpleJob {

	driver.RLock()
	defer driver.RUnlock()
	query := M{"location": location}
	jobs, err := driver.engine.readSimpleJobs(query)
	if err != nil {
		logger.ERROR("[#cache#] engine read simple jobs %+v error, %s", query, err.Error())
		return []*models.SimpleJob{}
	}
	return jobs
}

//GetSimpleJob is exported
func (driver *MongoStorageDriver) GetSimpleJob(jobid string) *models.SimpleJob {

	driver.RLock()
	defer driver.RUnlock()
	job, err := driver.engine.getSimpleJob(jobid)
	if err != nil {
		logger.ERROR("[#cache#] engine get simple job %s error, %s", jobid, err.Error())
		return nil
	}
	return job
}

//GetJobs is exported
func (driver *MongoStorageDriver) GetJobs() []*models.Job {

	driver.RLock()
	defer driver.RUnlock()
	query := M{}
	jobs, err := driver.engine.readJobs(query)
	if err != nil {
		logger.ERROR("[#cache#] engine read jobs %+v error, %s", query, err.Error())
		return []*models.Job{}
	}
	return jobs
}

//GetStateJobs is exported
func (driver *MongoStorageDriver) GetStateJobs(state int) []*models.Job {

	driver.RLock()
	defer driver.RUnlock()
	query := M{"stat": state}
	jobs, err := driver.engine.readJobs(query)
	if err != nil {
		logger.ERROR("[#cache#] engine read jobs %+v error, %s", query, err.Error())
		return []*models.Job{}
	}
	return jobs
}

//GetLocationJobs is exported
func (driver *MongoStorageDriver) GetLocationJobs(location string) []*models.Job {

	driver.RLock()
	defer driver.RUnlock()
	query := M{"location": location}
	jobs, err := driver.engine.readJobs(query)
	if err != nil {
		logger.ERROR("[#cache#] engine read jobs %+v error, %s", query, err.Error())
		return []*models.Job{}
	}
	return jobs
}

//GetGroupJobs is exported
func (driver *MongoStorageDriver) GetGroupJobs(groupid string) []*models.Job {

	driver.RLock()
	defer driver.RUnlock()
	query := M{"groupid": groupid}
	jobs, err := driver.engine.readJobs(query)
	if err != nil {
		logger.ERROR("[#cache#] engine read jobs %+v error, %s", query, err.Error())
		return []*models.Job{}
	}
	return jobs
}

//GetJob is exported
func (driver *MongoStorageDriver) GetJob(jobid string) *models.Job {

	driver.RLock()
	defer driver.RUnlock()
	job, err := driver.engine.getJob(jobid)
	if err != nil {
		logger.ERROR("[#cache#] engine get job %s error, %s", jobid, err.Error())
		return nil
	}
	return job
}

//SetJob is exported
func (driver *MongoStorageDriver) SetJob(job *models.Job) {

	driver.Lock()
	defer driver.Unlock()
	if err := driver.engine.putJob(job); err != nil {
		logger.ERROR("[#cache#] engine set job %s error, %s", job.JobId, err.Error())
	}
}

//SetJobLog is exported
func (driver *MongoStorageDriver) SetJobLog(joblog *models.JobLog) {

	driver.Lock()
	defer driver.Unlock()
	if err := driver.engine.postJobLog(joblog); err != nil {
		logger.ERROR("[#cache#] engine post job %s error, %s", joblog.JobId, err.Error())
	}
}
