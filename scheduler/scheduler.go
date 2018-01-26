package scheduler

import "github.com/cloudtask/cloudtask-center/cache"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gzkwrapper"
import "github.com/cloudtask/libtools/gounits/logger"

/*
 任务调配器
 当worker注册、注销或异常掉线时，该模块负责根据配置调整job分配
 Alloc；hash和pref两种模式，见配置.
   hash，哈希调配模式，以jobid为一致性hash key 调整重新计算新的worker.
         worker上线，workercache环发生变化，此时将该location环中的所有job（状态为STATE_STARTED的job除外）重新调配.
         worker下线，workercache环发生变化，为避免该location环中的所有job调配抖动过大，只调配掉线节点的job，重新计算hash.
   pref, 性能调配模式，该模式为定时轮询检查worker的job负载，如果某worker的job数过多，那么尽量将该worker的job分担到其余相
         对job较少的worker上，意味着该模式会定期调整，job执行worker随意度比较活跃.
         注：当worker上下线，该模式会均匀按任务数调整（状态为STATE_STARTED的job除外）.
*/

//Nodes is exported
type Nodes struct {
	Online  gzkwrapper.NodesPair
	Offline gzkwrapper.NodesPair
}

//NewNodes is exported
func NewNodes() *Nodes {

	return &Nodes{
		Online:  make(gzkwrapper.NodesPair),
		Offline: make(gzkwrapper.NodesPair),
	}
}

//Scheduler is exported
type Scheduler struct {
	allocMode       string
	cacheRepository *cache.CacheRepository
}

//NewScheduler is exported
func NewScheduler(allocMode string, cacheRepository *cache.CacheRepository) *Scheduler {

	return &Scheduler{
		allocMode:       allocMode,
		cacheRepository: cacheRepository,
	}
}

//QuickAlloc is exported
//以节点状态改变为条件进行分配，调度器对上下线节点进行job调整.
func (scheduler *Scheduler) QuickAlloc(online gzkwrapper.NodesPair, offline gzkwrapper.NodesPair) {

	logger.INFO("[#scheduler#] quick alloc. %s", scheduler.allocMode)
	nodesMapper := make(map[string]*Nodes, 0)
	for key, nodedata := range online {
		location := nodedata.Location
		if _, ret := nodesMapper[location]; !ret {
			nodesMapper[location] = NewNodes()
		}
		nodesMapper[location].Online[key] = nodedata
	}

	for key, nodedata := range offline {
		location := nodedata.Location
		if _, ret := nodesMapper[location]; !ret {
			nodesMapper[location] = NewNodes()
		}
		nodesMapper[location].Offline[key] = nodedata
	}

	if scheduler.allocMode == "hash" {
		for location, nodes := range nodesMapper {
			logger.INFO("[#scheduler#] hash alloc %s.", location)
			scheduler.HashAlloc(location, nodes)
		}
		return
	}

	if scheduler.allocMode == "pref" {
		for location, nodes := range nodesMapper {
			logger.INFO("[#scheduler#] pref alloc %s.", location)
			scheduler.PrefAlloc(location, nodes)
		}
		return
	}
	logger.WARN("[#scheduler#] alloc mode invalid, jobs can't adjust. :-(")
}

//SingleJobAlloc is exported
//以job为条件进行分配，当创建或删除Job时调度器会立即分配
func (scheduler *Scheduler) SingleJobAlloc(location string, jobid string) {

	logger.INFO("[#scheduler#] single alloc. %s", scheduler.allocMode)
	if scheduler.allocMode == "hash" {
		scheduler.HashSingleJobAlloc(location, jobid)
		return
	}

	if scheduler.allocMode == "pref" {
		scheduler.PrefSingleJobAlloc(location, jobid)
		return
	}
	logger.WARN("[#scheduler#] alloc mode invalid, jobs can't adjust. :-(")
}

//RecoveryLocationAlloc is exported
//检查location下所有enabled已打开的job
//1、若分配表中不存在，则重新分配一次.
//2、已存在，则检测分配的节点是否已发生改变，若改变则重新再分配.
func (scheduler *Scheduler) RecoveryLocationAlloc(location string, jobs []*models.SimpleJob) {

	logger.INFO("[#scheduler#] recovery %s alloc.", location)
	jobKeys := make(map[string]string, 0)
	for _, job := range jobs {
		if job.Enabled == 1 {
			found := scheduler.cacheRepository.HasAllocJobId(job.Location, job.JobId)
			if !found { //不在分配表中，可能在创建job后通过HashSingleJobAlloc分配失败过1次，立即重新分配
				worker := scheduler.cacheRepository.ChoiceWorker(location, job.JobId, job.Servers) //hash重新分配
				if worker != nil {
					scheduler.cacheRepository.CreateAllocJob(location, worker.Key, job.JobId) //加入到分配表
					scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_STOPED)
					logger.INFO("[#scheduler#] reset %s job %s to joballoc successed, select worker %s.", location, job.JobId, worker.Key)
				} else {
					if job.Stat != models.STATE_REALLOC {
						scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_REALLOC)
						logger.ERROR("[#scheduler#] reset %s job %s to joballoc failed, select worker invalid.", location, job.JobId)
					}
				}
			} else { //分配表中存在，对比worker.Key检查是否需要重新分配.
				jobData := scheduler.cacheRepository.GetAllocJob(location, job.JobId)
				if jobData != nil {
					worker := scheduler.cacheRepository.ChoiceWorker(location, job.JobId, job.Servers)
					if worker != nil {
						if worker.Key != jobData.Key { //节点发生变化了，需要调整
							jobKeys[job.JobId] = worker.Key
							logger.INFO("[#scheduler#] reset %s job %s to joballoc successed, select worker %s.", location, job.JobId, worker.Key)
						}
					} else {
						if job.Stat != models.STATE_REALLOC {
							scheduler.cacheRepository.RemoveAllocJob(location, job.JobId) //分配失败，从分配表删除.
							scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_REALLOC)
							logger.ERROR("[#scheduler#] reset %s job %s to joballoc failed, select worker invalid.", location, job.JobId)
						}
					}
				}
			}
		}
	}

	//分配表中存在同时分配节点发生过变化，将其批量修改分配表.
	if nSize := len(jobKeys); nSize > 0 {
		logger.INFO("[#scheduler#] reset %s jobs %d to joballoc successed.", location, nSize)
		scheduler.cacheRepository.SetAllocJobsKey(location, jobKeys)
	}
}
