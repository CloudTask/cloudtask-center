package scheduler

import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/logger"

/*
  HashAlloc
  hash分配原则：
  1、若location下只有worker上线情况(online > 0, offline == 0)
     location中worker环发生了变化, 将环中所有worker的jobs重新分配一次, 状态为STARTED的job排除在外.
     将job的状态设置为REALLOC.

  2、若location下只有worker下线情况(online == 0, offline > 0)
     location中worker环虽然发生了变化，但为了避免location下所有jobs全都调整(抖动过大)，因此只处理下线worker的所有jobs.
     将job的状态设置为REALLOC.

  3、若location下既有worker上线，也有worker下线(online > 0, offline > 0)
	 记录下线worker的jobs到临时缓存(tempOffJobs)
	 将location中所有worker的job重新分配一次，但状态为STARTED同时在tempOffJobs中未查询到的job排除在外.
	 在tempOffJobs中的job虽然为启动状态，但有可能worker是异常关闭没有改变job状态，所以必须重新分配.
	 将job的状态设置为REALLOC.
*/
func (scheduler *Scheduler) HashAlloc(location string, nodes *Nodes) {

	logger.INFO("[#scheduler#] hash-alloc. %s", location)
	jobsAlloc := scheduler.cacheRepository.GetAlloc(location)
	if jobsAlloc == nil {
		logger.INFO("[#scheduler#] location %s not found, hash-alloc return.", location)
		return
	}

	onlineSize := len(nodes.Online)
	offlineSize := len(nodes.Offline)
	if onlineSize > 0 && offlineSize == 0 {
		scheduler.onlineJobsAlloc(location, nodes, jobsAlloc)
		jobs := scheduler.cacheRepository.GetLocationSimpleJobs(location)
		scheduler.RecoveryLocationAlloc(location, jobs)
		return
	}

	if onlineSize == 0 && offlineSize > 0 {
		scheduler.offlineJobsAlloc(location, nodes, jobsAlloc)
		jobs := scheduler.cacheRepository.GetLocationSimpleJobs(location)
		scheduler.RecoveryLocationAlloc(location, jobs)
		return
	}

	if onlineSize > 0 && offlineSize > 0 {
		scheduler.allJobsAlloc(location, nodes, jobsAlloc)
		jobs := scheduler.cacheRepository.GetLocationSimpleJobs(location)
		scheduler.RecoveryLocationAlloc(location, jobs)
		return
	}
}

/*
  HashSingleJobAlloc: 调整单一job
  条件：当创建或修改job后调用该方法
*/
func (scheduler *Scheduler) HashSingleJobAlloc(location string, jobid string) {

	logger.INFO("[#scheduler#] hashsingle-joballoc %s job %s", location, jobid)
	job := scheduler.cacheRepository.GetSimpleJob(jobid)
	if job == nil {
		logger.INFO("[#scheduler#] %s job %s not found, return.", location, jobid)
		return
	}

	if job.Enabled == 0 {
		logger.INFO("[#scheduler#] %s job %s enabled is disabled, return.", location, job.JobId)
		return
	}

	jobData := scheduler.cacheRepository.GetAllocJob(location, job.JobId)
	if jobData == nil {
		worker := scheduler.cacheRepository.ChoiceWorker(location, job.JobId, job.Servers)
		if worker != nil {
			scheduler.cacheRepository.CreateAllocJob(location, worker.Key, job.JobId) //创建到任务分配表
			scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_STOPED)     //修改任务状态为：STATE_STOPED.
			logger.INFO("[#scheduler#] create %s job %s to joballoc successed, select worker %s.", location, job.JobId, worker.Key)
		} else {
			if job.Stat != models.STATE_REALLOC {
				scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_REALLOC) //无法选择worker分配，修改任务状态为：STATE_REALLOC.
				logger.ERROR("[#scheduler#] create %s job %s to joballoc failed, select worker invalid.", location, job.JobId)
			}
		}
	} else {
		if job.Stat != models.STATE_STARTED {
			worker := scheduler.cacheRepository.ChoiceWorker(location, job.JobId, job.Servers)
			if worker != nil {
				if worker.Key != jobData.Key { //节点发生变化了，需要调整
					jobs := make(map[string]string, 0)
					jobs[job.JobId] = worker.Key
					logger.INFO("[#scheduler#] reset %s job %s to joballoc successed, select worker %s.", location, job.JobId, worker.Key)
					scheduler.cacheRepository.SetAllocJobsKey(location, jobs) //修改任务分配表
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

/*
  onlineJobsAlloc: 上线节点job分配算法
  location中状态为非STATE_STARTED的所有jobs重新分配
  如果只有一个节点上线，workercache中的worker数为1，则忽略job状态，全部重新分配一次
*/
func (scheduler *Scheduler) onlineJobsAlloc(location string, nodes *Nodes, jobsAlloc *models.JobsAlloc) {

	logger.INFO("[#scheduler#] online-jobsalloc. %s", location)
	jobKeys := make(map[string]string, 0)
	for _, jobData := range jobsAlloc.Jobs {
		job := scheduler.cacheRepository.GetSimpleJob(jobData.JobId)
		if job != nil && job.Stat != models.STATE_STARTED {
			worker := scheduler.cacheRepository.ChoiceWorker(location, job.JobId, job.Servers) //hash重新分配
			if worker != nil {
				if worker.Key != jobData.Key {
					jobKeys[job.JobId] = worker.Key
					logger.INFO("[#scheduler#] reset %s job %s to joballoc successed, select worker %s.", location, job.JobId, worker.Key)
				}
				if job.Stat == models.STATE_REALLOC { //之前是等待分配状态的任务全部改为停止，其余状态保持不变
					scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_STOPED)
				}
			} else {
				scheduler.cacheRepository.RemoveAllocJob(location, job.JobId) //分配失败，从分配表删除.
				scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_REALLOC)
				logger.ERROR("[#scheduler#] reset %s job %s to joballoc failed, select worker invalid.", location, job.JobId)
			}
		}
	}

	if nSize := len(jobKeys); nSize > 0 { //提交分配表
		logger.INFO("[#scheduler#] reset %s jobs %d to joballoc successed.", location, nSize)
		scheduler.cacheRepository.SetAllocJobsKey(location, jobKeys) //修改任务分配表
	}
}

/*
  offlineJobsAlloc: 下线节点job分配算法
  下线节点的所有job重新分配到新节点
*/
func (scheduler *Scheduler) offlineJobsAlloc(location string, nodes *Nodes, jobsAlloc *models.JobsAlloc) {

	logger.INFO("[#scheduler#] offline-jobsalloc. %s", location)
	jobKeys := make(map[string]string, 0)
	for key := range nodes.Offline { //只处理下线节点
		jobIds := scheduler.cacheRepository.GetAllocJobIds(location, key) //获取分配表中该节点的所有任务
		for _, jobId := range jobIds {
			job := scheduler.cacheRepository.GetSimpleJob(jobId)
			if job != nil {
				worker := scheduler.cacheRepository.ChoiceWorker(location, job.JobId, job.Servers) //重新分配节点
				if worker != nil {
					jobKeys[job.JobId] = worker.Key
					scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_STOPED)
					logger.INFO("[#scheduler#] reset %s job %s to joballoc successed, select worker %s.", location, job.JobId, worker.Key)
				} else {
					scheduler.cacheRepository.RemoveAllocJob(location, job.JobId)
					scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_REALLOC)
					logger.ERROR("[#scheduler#] reset %s job %s to joballoc failed, select worker invalid.", location, job.JobId)
				}
			}
		}
	}

	if nSize := len(jobKeys); nSize > 0 { //提交分配表
		logger.INFO("[#scheduler#] reset %s jobs %d to joballoc successed.", location, nSize)
		scheduler.cacheRepository.SetAllocJobsKey(location, jobKeys)
	}
}

/*
  allJobsAlloc: 即有上线也有下线节点job分配算法
  记录下线节点的所有jobids
  当状态为STATE_STARTED，但在tempOffJobs中找到后也要重新分配
*/
func (scheduler *Scheduler) allJobsAlloc(location string, nodes *Nodes, jobsAlloc *models.JobsAlloc) {

	logger.INFO("[#scheduler#] all-jobsalloc. %s", location)
	//找出离线节点所有的jobid
	tempOffJobs := []string{}
	for key := range nodes.Offline {
		jobIds := scheduler.cacheRepository.GetAllocJobIds(location, key) //获取分配表中离线节点的所有任务
		if len(jobIds) > 0 {
			tempOffJobs = append(tempOffJobs, jobIds...)
		}
	}

	jobKeys := make(map[string]string, 0)
	for _, jobData := range jobsAlloc.Jobs {
		job := scheduler.cacheRepository.GetSimpleJob(jobData.JobId)
		found := false
		for _, jobId := range tempOffJobs {
			if jobId == jobData.JobId {
				found = true
				break
			}
		}
		if job != nil && job.Stat != models.STATE_STARTED || (job.Stat == models.STATE_STARTED && found) {
			worker := scheduler.cacheRepository.ChoiceWorker(location, job.JobId, job.Servers) //hash重新分配
			if worker != nil {
				if worker.Key != jobData.Key {
					jobKeys[job.JobId] = worker.Key
					logger.INFO("[#scheduler#] reset %s job %s to joballoc successed, select worker %s.", location, job.JobId, worker.Key)
				}
				if job.Stat == models.STATE_REALLOC || (job.Stat == models.STATE_STARTED && found) {
					scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_STOPED)
				}
			} else {
				scheduler.cacheRepository.RemoveAllocJob(location, job.JobId) //分配失败，从分配表删除.
				scheduler.cacheRepository.SetJobState(job.JobId, models.STATE_REALLOC)
				logger.ERROR("[#scheduler#] reset %s job %s to joballoc failed, select worker invalid.", location, job.JobId)
			}
		}
	}

	if nSize := len(jobKeys); nSize > 0 { //提交分配表
		logger.INFO("[#scheduler#] reset %s jobs %d to joballoc successed.", location, nSize)
		scheduler.cacheRepository.SetAllocJobsKey(location, jobKeys)
	}
}
