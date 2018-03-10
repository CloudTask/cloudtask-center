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
	//中心服务器不实现心跳
}

func (server *CenterServer) OnAllocCacheHandlerFunc(event cache.AllocEvent, location string, data []byte, err error) {

	if err != nil {
		logger.ERROR("[#server#] %s cache handler %s error, %s.", location, event.String(), err)
		return
	}

	if len(data) <= 0 {
		logger.ERROR("[#server#] %s cache handler %s error, alloc data invalid.", location, event.String())
		return
	}

	logger.INFO("[#server#] %s cache handler %s.", location, event.String())
	switch event {
	case cache.ALLOC_CREATED_EVENT:
		{
			server.postCacheAlloc(location, data)
		}
	case cache.ALLOC_CHANGED_EVENT:
		{
			server.putCacheAlloc(location, data)
		}
	case cache.ALLOC_REMOVED_EVENT:
		{
			server.putCacheAlloc(location, data)
		}
	}
}

func (server *CenterServer) OnNodeCacheHandlerFunc(event cache.NodeEvent, location string, worker *cache.Worker) {

	logger.INFO("[#server#] cache handler %s %s, worker %s, %s(%s).", location, event.String(), worker.Key, worker.Name, worker.IPAddr)
}
