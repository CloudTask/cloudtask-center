package driver

import "github.com/cloudtask/cloudtask-center/cache/driver/types"
import "github.com/cloudtask/common/models"

import (
	"fmt"
	"sort"
	"strings"
)

type Initialize func(parameters types.Parameters) (StorageDriver, error)

var (
	initializes      = make(map[types.Backend]Initialize)
	supportedBackend = func() string {
		keys := make([]string, 0, len(initializes))
		for k := range initializes {
			keys = append(keys, string(k))
		}
		sort.Strings(keys)
		return strings.Join(keys, ",")
	}()
)

//StorageDriver is exported
type StorageDriver interface {
	Open() error
	Close()
	GetLocationsName() []string
	GetLocation(location string) *models.WorkLocation
	GetLocationServer(location string, key string) *models.Server
	GetLocationServers(location string) []*models.Server
	CreateLocationServer(location string, key string, hostname string, ipaddr string, apiaddr string, os string, platform string) *models.Server
	ChangeLocationServer(location string, key string, hostname string, ipaddr string, apiaddr string, os string, platform string) *models.Server
	RemoveLocationServer(location string, key string)
	GetLocationSimpleJobs(location string) []*models.SimpleJob
	GetSimpleJob(jobid string) *models.SimpleJob
	GetJobs() []*models.Job
	GetStateJobs(state int) []*models.Job
	GetLocationJobs(location string) []*models.Job
	GetGroupJobs(groupid string) []*models.Job
	GetJob(jobid string) *models.Job
	SetJob(job *models.Job)
	SetJobLog(joblog *models.JobLog)
}

func NewDriver(backend types.Backend, parameters types.Parameters) (StorageDriver, error) {
	if init, exists := initializes[backend]; exists {
		return init(parameters)
	}
	return nil, fmt.Errorf("%s %s", types.ErrDriverNotSupported, supportedBackend)
}

func AddDriver(backend types.Backend, init Initialize) {
	initializes[backend] = init
}
