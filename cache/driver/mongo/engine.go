package mongo

import "github.com/cloudtask/cloudtask-center/cache/driver/types"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/system"
import "gopkg.in/mgo.v2/bson"
import mgo "gopkg.in/mgo.v2"

import (
	"strings"
	"time"
)

const (
	defaultMaxPoolSize = 20
)

const (
	SYS_LOCATIONNS = "sys_locations"
	SYS_JOBS       = "sys_jobs"
	SYS_LOGS       = "logs"
)

type M bson.M

type D bson.D

//MgoConfigs is exported
type MgoConfigs struct {
	Hosts    string
	DataBase string
	Auth     map[string]string
	Options  []string
}

//Engine is exported
type Engine struct {
	MgoConfigs
	globalSession  *mgo.Session
	failPulseTimes int
	stopCh         chan struct{}
}

//NewEngine is exported
func NewEngine(configs MgoConfigs) *Engine {

	return &Engine{
		MgoConfigs: configs,
		stopCh:     make(chan struct{}),
	}
}

func generateHostURL(configs MgoConfigs) (string, error) {

	configs.Hosts = strings.TrimSpace(configs.Hosts)
	if len(configs.Hosts) == 0 {
		return "", ErrMongoStorageDriverHostsInvalid
	}

	configs.DataBase = strings.TrimSpace(configs.DataBase)
	if len(configs.DataBase) == 0 {
		return "", ErrMongoStorageDriverDataBaseInvalid
	}

	var authStr string
	if len(configs.Auth) > 0 {
		var (
			user, password string
			ret            bool
		)
		if user, ret = configs.Auth["user"]; ret {
			authStr = user
			if password, ret = configs.Auth["password"]; ret {
				authStr = authStr + ":" + password
			}
			authStr = authStr + "@"
		}
	}

	var optsStr string
	if len(configs.Options) > 0 {
		for index, value := range configs.Options {
			optsStr = optsStr + value
			if index != len(configs.Options)-1 {
				optsStr = optsStr + "&"
			}
		}
	}

	mgoURL := "mongodb://" + authStr + configs.Hosts + "/" + configs.DataBase
	if optsStr != "" {
		mgoURL = mgoURL + "?" + optsStr
	}

	if _, err := mgo.ParseURL(mgoURL); err != nil {
		return "", err
	}
	return mgoURL, nil
}

//Open is exported
func (engine *Engine) Open() error {

	var maxPoolSize = defaultMaxPoolSize
	opts := system.DriverOpts(engine.Options)
	if value, ret := opts.Int("maxPoolSize", ""); ret {
		maxPoolSize = (int)(value)
	}

	mgoURL, err := generateHostURL(engine.MgoConfigs)
	if err != nil {
		return err
	}

	session, err := mgo.Dial(mgoURL)
	if err != nil {
		return err
	}

	session.SetMode(mgo.Strong, true)
	session.SetPoolLimit(maxPoolSize)
	engine.globalSession = session
	go engine.pulseSessionLoop()
	return nil
}

//Close is exported
func (engine *Engine) Close() {

	close(engine.stopCh)
	if engine.globalSession != nil {
		engine.globalSession.Close()
	}
}

func (engine *Engine) getLocation(location string) (*models.WorkLocation, error) {

	session := engine.getSession()
	defer session.Close()
	workLocation := &models.WorkLocation{}
	if err := session.DB(engine.DataBase).C(SYS_LOCATIONNS).
		Find(M{"location": location}).
		Select(M{"_id": 0}).One(workLocation); err != nil {
		if err == mgo.ErrNotFound {
			return nil, types.ErrDriverResourceNotFound
		}
		return nil, err
	}
	return workLocation, nil
}

func (engine *Engine) postLocation(workLocation *models.WorkLocation) error {

	session := engine.getSession()
	defer session.Close()
	return session.DB(engine.DataBase).C(SYS_LOCATIONNS).
		Insert(workLocation)
}

func (engine *Engine) putLocation(workLocation *models.WorkLocation) error {

	session := engine.getSession()
	defer session.Close()
	return session.DB(engine.DataBase).C(SYS_LOCATIONNS).
		Update(M{"location": workLocation.Location}, workLocation)
}

func (engine *Engine) readLocationsName() ([]string, error) {

	session := engine.getSession()
	defer session.Close()
	workLocations := []*models.WorkLocation{}
	if err := session.DB(engine.DataBase).C(SYS_LOCATIONNS).
		Find(M{}).
		Select(M{"_id": 0, "location": 1}).
		All(&workLocations); err != nil {
		return nil, err
	}

	names := []string{}
	for _, workLocation := range workLocations {
		names = append(names, workLocation.Location)
	}
	return names, nil
}

func (engine *Engine) readSimpleJobs(query M) ([]*models.SimpleJob, error) {

	session := engine.getSession()
	defer session.Close()
	jobs := []*models.SimpleJob{}
	if err := session.DB(engine.DataBase).C(SYS_JOBS).
		Find(query).
		Select(M{"_id": 0, "jobid": 1, "name": 1, "location": 1, "groupid": 1, "servers": 1, "enabled": 1, "stat": 1}).
		All(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (engine *Engine) readJobs(query M) ([]*models.Job, error) {

	session := engine.getSession()
	defer session.Close()
	jobs := []*models.Job{}
	if err := session.DB(engine.DataBase).C(SYS_JOBS).
		Find(query).
		Select(M{"_id": 0}).
		All(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (engine *Engine) getSimpleJob(jobid string) (*models.SimpleJob, error) {

	job, err := engine.getJob(jobid)
	if err != nil {
		return nil, err
	}

	return &models.SimpleJob{
		JobId:    job.JobId,
		Name:     job.Name,
		Location: job.Location,
		GroupId:  job.GroupId,
		Servers:  job.Servers,
		Enabled:  job.Enabled,
		Stat:     job.Stat,
	}, nil
}

func (engine *Engine) getJob(jobid string) (*models.Job, error) {

	session := engine.getSession()
	defer session.Close()
	job := &models.Job{}
	if err := session.DB(engine.DataBase).C(SYS_JOBS).
		Find(M{"jobid": jobid}).
		Select(M{"_id": 0}).One(job); err != nil {
		if err == mgo.ErrNotFound {
			return nil, types.ErrDriverResourceNotFound
		}
		return nil, err
	}
	return job, nil
}

func (engine *Engine) putJob(job *models.Job) error {

	session := engine.getSession()
	defer session.Close()
	return session.DB(engine.DataBase).C(SYS_JOBS).
		Update(M{"jobid": job.JobId}, M{"$set": M{
			"stat":    job.Stat,
			"execerr": job.ExecErr,
			"execat":  job.ExecAt,
			"nextat":  job.NextAt}})
}

func (engine *Engine) postJobLog(jobLog *models.JobLog) error {

	session := engine.getSession()
	defer session.Close()
	return session.DB(engine.DataBase).C(SYS_LOGS).
		Insert(jobLog)
}

func (engine *Engine) getSession() *mgo.Session {

	return engine.globalSession.Clone()
}

func (engine *Engine) pulseSessionLoop() {

	for {
		pulseTicker := time.NewTicker(time.Second * 90)
		select {
		case <-pulseTicker.C:
			{
				pulseTicker.Stop()
				session := engine.getSession()
				if err := session.Ping(); err != nil {
					if engine.failPulseTimes > 3 {
						engine.globalSession.Refresh()
						engine.failPulseTimes = 0
					} else {
						engine.failPulseTimes = engine.failPulseTimes + 1
					}
				} else {
					engine.failPulseTimes = 0
				}
				session.Close()
			}
		case <-engine.stopCh:
			{
				pulseTicker.Stop()
				return
			}
		}
	}
}
