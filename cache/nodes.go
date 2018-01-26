package cache

import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/algorithm"

import (
	"hash/crc32"
	"sort"
	"strings"
	"sync"
)

//EventType is exported
type EventType int

const (
	CREATE_WORKER EventType = iota + 1
	RELEASE_WORKER
	CHANGE_WORKER
)

func (event EventType) String() string {

	switch event {
	case CREATE_WORKER:
		return "CREATE_WORKER"
	case RELEASE_WORKER:
		return "RELEASE_WORKER"
	case CHANGE_WORKER:
		return "CHANGE_WORKER"
	}
	return ""
}

//Worker is exported
type Worker struct {
	Location string
	*models.AttachData
	*models.Server
}

//NodeStore is exported
type NodeStore struct {
	sync.RWMutex
	nodesData map[string][]*Worker
	circle    map[string]*algorithm.Consistent
	ICacheRepositoryHandler
}

//NewNodeStore is exported
func NewNodeStore(handler ICacheRepositoryHandler) *NodeStore {

	return &NodeStore{
		nodesData:               make(map[string][]*Worker, 0),
		circle:                  make(map[string]*algorithm.Consistent, 0),
		ICacheRepositoryHandler: handler,
	}
}

//GetWorker is exported
func (store *NodeStore) GetWorker(key string) *Worker {

	store.RLock()
	defer store.RUnlock()
	for _, workers := range store.nodesData {
		for _, worker := range workers {
			if worker.Key == key {
				return worker
			}
		}
	}
	return nil
}

//GetWorkers is exported
func (store *NodeStore) GetWorkers(location string) []*Worker {

	store.RLock()
	defer store.RUnlock()
	workers, ret := store.nodesData[location]
	if !ret {
		return []*Worker{}
	}
	return workers
}

//HashLocationWorker is exported
func (store *NodeStore) HashLocationWorker(location string, key string) *Worker {

	var (
		ret     bool
		workers []*Worker
	)

	store.RLock()
	defer store.RUnlock()
	if workers, ret = store.nodesData[location]; !ret {
		return nil
	}

	c := store.circle[location]
	if c == nil {
		return nil
	}

	workerKey := c.Get(key)
	for _, worker := range workers {
		if worker.Key == workerKey {
			return worker
		}
	}
	return nil
}

//HashLocationRangeWorker is exported
func (store *NodeStore) HashLocationRangeWorker(location string, key string, servers []string) *Worker {

	store.RLock()
	defer store.RUnlock()
	workerKeys := []string{}
	selectWorkers := make(map[string]*Worker)
	workers := store.GetWorkers(location)
	for _, server := range servers {
		for _, worker := range workers {
			if server == worker.IPAddr || strings.ToUpper(server) == strings.ToUpper(worker.Name) {
				workerKeys = append(workerKeys, worker.Key)
				selectWorkers[worker.Key] = worker
			}
		}
	}

	size := len(workerKeys)
	if size > 0 {
		sort.Strings(workerKeys)
		index := crc32.ChecksumIEEE([]byte(key)) % (uint32)(size)
		workerKey := workerKeys[index]
		return selectWorkers[workerKey]
	}
	return nil
}

//ClearWorkers is exported
func (store *NodeStore) ClearWorkers() {

	store.Lock()
	defer store.Unlock()
	for location := range store.nodesData {
		store.nodesData[location] = []*Worker{}
		delete(store.nodesData, location)
		delete(store.circle, location)
	}
}

//CreateWorker is exported
func (store *NodeStore) CreateWorker(location string, attach *models.AttachData, server *models.Server) {

	var (
		ret     bool
		workers []*Worker
		w       = &Worker{
			Location:   location,
			AttachData: attach,
			Server:     server,
		}
	)

	store.Lock()
	defer store.Unlock()
	added := false
	workers, ret = store.nodesData[location]
	if !ret {
		added = true
		store.nodesData[location] = []*Worker{w}
		c := algorithm.NewConsisten(50)
		c.Add(w.Key)
		store.circle[location] = c
	} else {
		found := false
		for _, worker := range workers {
			if worker.Key == w.Key {
				found = true
				break
			}
		}
		if !found {
			added = true
			store.nodesData[location] = append(store.nodesData[location], w)
			store.circle[location].Add(w.Key)
		}
	}

	if added {
		store.OnNodeCacheChangedHandlerFunc(CREATE_WORKER, location, w)
	}
}

//ChangeWorker is exported
func (store *NodeStore) ChangeWorker(location string, attach *models.AttachData, server *models.Server) {

	var (
		ret     bool
		workers []*Worker
	)

	store.Lock()
	defer store.Unlock()
	if workers, ret = store.nodesData[location]; !ret {
		return
	}

	for i := range workers {
		if workers[i].Key == server.Key {
			workers[i].Server = server
			workers[i].AttachData = attach
			store.nodesData[location] = workers
			store.OnNodeCacheChangedHandlerFunc(CHANGE_WORKER, location, workers[i])
			break
		}
	}
}

//RemoveWorker is exported
func (store *NodeStore) RemoveWorker(location string, key string) {

	var (
		ret     bool
		workers []*Worker
	)

	store.Lock()
	defer store.Unlock()
	if workers, ret = store.nodesData[location]; !ret {
		return
	}

	for i, worker := range workers {
		if worker.Key == key {
			store.nodesData[location] = append(store.nodesData[location][:i], store.nodesData[location][i+1:]...)
			if len(store.nodesData[location]) == 0 {
				delete(store.nodesData, location)
			}
			store.circle[location].Remove(key)
			if len(store.circle[location].Members()) == 0 {
				delete(store.circle, location)
			}
			store.OnNodeCacheChangedHandlerFunc(RELEASE_WORKER, location, workers[i])
			break
		}
	}
}

//RemoveLocation is exported
func (store *NodeStore) RemoveLocation(location string) {

	store.Lock()
	delete(store.nodesData, location)
	delete(store.circle, location)
	store.OnNodeCacheLocationRemovedHandlerFunc(location)
	store.Unlock()
}
