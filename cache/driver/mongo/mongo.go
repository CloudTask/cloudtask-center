package mongo

import "github.com/cloudtask/cloudtask-center/cache/driver"
import "github.com/cloudtask/cloudtask-center/cache/driver/types"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/logger"

import (
	"errors"
	"sync"
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

	mgoConfigs, err := parseEngineConfigs(parameters)
	if err != nil {
		return nil, err
	}

	return &MongoStorageDriver{
		engine: NewEngine(mgoConfigs),
	}, nil
}

func parseEngineConfigs(parameters types.Parameters) (MgoConfigs, error) {

	var (
		ret   bool
		value interface{}
	)

	mgoConfigs := MgoConfigs{
		Auth:    map[string]string{},
		Options: []string{},
	}

	value, ret = parameters["hosts"]
	if !ret {
		return mgoConfigs, ErrMongoStorageDriverHostsInvalid
	}
	mgoConfigs.Hosts = value.(string)

	value, ret = parameters["database"]
	if !ret {
		return mgoConfigs, ErrMongoStorageDriverDataBaseInvalid
	}
	mgoConfigs.DataBase = value.(string)

	if value, ret = parameters["auth"]; ret {
		if auth, ok := value.(map[string]interface{}); ok {
			if user, ok := auth["user"]; ok {
				mgoConfigs.Auth["user"] = user.(string)
			}
			if password, ok := auth["password"]; ok {
				mgoConfigs.Auth["password"] = password.(string)
			}
		}
	}

	if value, ret = parameters["options"]; ret {
		if options, ok := value.([]interface{}); ok {
			for _, option := range options {
				mgoConfigs.Options = append(mgoConfigs.Options, option.(string))
			}
		}
	}
	return mgoConfigs, nil
}

//Open is exported
func (driver *MongoStorageDriver) Open() error {

	return driver.engine.Open()
}

//Close is exported
func (driver *MongoStorageDriver) Close() {

	driver.engine.Close()
}

//SetConfigParameters is exported
func (driver *MongoStorageDriver) SetConfigParameters(parameters types.Parameters) {

	mgoConfigs, err := parseEngineConfigs(parameters)
	if err != nil {
		logger.ERROR("[#cache#] mongo driver parse configs error, %s", err.Error())
		return
	}

	err = driver.engine.SetConfigParameters(mgoConfigs)
	if err != nil {
		logger.ERROR("[#cache#] mongo driver set configs error, %s", err.Error())
		return
	}
	logger.ERROR("[#cache#] mongo driver configs changed, %+v", mgoConfigs)
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
