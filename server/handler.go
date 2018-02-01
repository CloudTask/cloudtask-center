package server

import "github.com/cloudtask/cloudtask-center/cache"
import "github.com/cloudtask/libtools/gounits/logger"
import "github.com/cloudtask/libtools/gzkwrapper"

func (server *CenterServer) OnZkWrapperNodeHandlerFunc(nodestore *gzkwrapper.NodeStore) {

	newTotalSize := nodestore.NewTotalSize()
	logger.INFO("[#server#] cluster discovery healthy nodes %d", newTotalSize)
	for key, nodedata := range nodestore.New {
		logger.INFO("[#server#] %s node %s(%s) healthy.", nodedata.Location, key, nodedata.IpAddr)
		server.CacheRepository.CreateWorker(key, nodedata)
		server.CacheRepository.InitAllocBuffer(nodedata.Location, nil)
	}

	deadTotalSize := nodestore.DeadTotalSize()
	logger.INFO("[#server#] cluster discovery deadly nodes %d", deadTotalSize)
	for key, nodedata := range nodestore.Dead {
		logger.INFO("[#server#] %s node %s(%s) deadly.", nodedata.Location, key, nodedata.IpAddr)
		server.CacheRepository.RemoveWorker(key, nodedata)
	}

	recoveryTotalSize := nodestore.RecoveryTotalSize()
	logger.INFO("[#server#] cluster discovery recovery nodes %d", recoveryTotalSize)
	for key, nodedata := range nodestore.Recovery {
		logger.INFO("[#server#] %s node %s(%s) recovery.", nodedata.Location, key, nodedata.IpAddr)
		server.CacheRepository.ChangeWorker(key, nodedata)
	}

	if newTotalSize > 0 || deadTotalSize > 0 {
		//worker上下线，开始调节重新分配任务到worker
		server.Scheduler.QuickAlloc(nodestore.New, nodestore.Dead)
		//worker上下线事件通知
		server.postNodesWatchNotifyEvent(nodestore.New, nodestore.Dead)
	}
}

func (server *CenterServer) OnZkWrapperPulseHandlerFunc(key string, nodedata *gzkwrapper.NodeData, err error) {
}

func (server *CenterServer) OnAllocCacheChangedHandlerFunc(location string, data []byte, err error) {

	if err != nil {
		logger.ERROR("[#server#] alloc cache changed handler error, %s", err)
		return
	}
	//logger.INFO("[#server#] alloc cache changed. %s %d", location, len(data))
	server.postCacheAlloc(location, data)
}

func (server *CenterServer) OnAllocCacheLocationRemovedHandlerFunc(location string) {

	//logger.INFO("[#server#] alloc cache location %s removed.", location)
	server.removeCacheAlloc(location)
}

func (server *CenterServer) OnNodeCacheChangedHandlerFunc(event cache.EventType, location string, worker *cache.Worker) {

	//logger.INFO("[#server#] node cache changed, event %s, %s %s, ipaddr %s.", event.String(), location, worker.Key, worker.IPAddr)
}

func (server *CenterServer) OnNodeCacheLocationRemovedHandlerFunc(location string) {

	//logger.INFO("[#server#] node cache location %s removed.", location)
}
