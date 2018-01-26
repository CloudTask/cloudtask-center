package api

import "github.com/cloudtask/cloudtask-center/cache"
import "github.com/cloudtask/cloudtask-center/scheduler"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/logger"

import (
	"encoding/json"
	"fmt"
)

//ProcessSystemEventMessage is exported
func ProcessSystemEventMessage(request *MessageRequest) error {

	logger.INFO("[#api#] process systemevent message %s", request.Header.MsgId)
	systemEvent := &models.SystemEvent{}
	if err := json.NewDecoder(request.Reader).Decode(systemEvent); err != nil {
		return fmt.Errorf("process systemevent message %s failure, %s", request.Header.MsgId, err.Error())
	}

	cacheRepository := request.Context.Get("CacheRepository").(*cache.CacheRepository)
	switch systemEvent.Event {
	case models.RemoveGroupEvent:
		{ //只考虑删除组情况，创建和修改组不会对分配表造成改变.
			logger.INFO("[#api#] ### %s, %+v", models.RemoveGroupEvent, systemEvent)
			cacheRepository.RemoveAllocJobs(systemEvent.Runtime, systemEvent.JobIds)
			cacheRepository.RemoveJobs(systemEvent.JobIds)
		}
	case models.CreateJobEvent:
		{ //创建新任务事件
			logger.INFO("[#api#] ### %s, %+v", models.CreateJobEvent, systemEvent)
			if len(systemEvent.JobIds) > 0 {
				jobId := systemEvent.JobIds[0]
				if job := cacheRepository.GetRawJob(jobId); job != nil {
					scheduler := request.Context.Get("Scheduler").(*scheduler.Scheduler)
					scheduler.SingleJobAlloc(systemEvent.Runtime, jobId)
				}
			}
		}
	case models.RemoveJobEvent:
		{ //删除一个任务事件
			logger.INFO("[#api#] ### %s, %+v", models.RemoveJobEvent, systemEvent)
			if len(systemEvent.JobIds) > 0 {
				jobId := systemEvent.JobIds[0]
				cacheRepository.RemoveAllocJob(systemEvent.Runtime, jobId) //从分配表删除
				cacheRepository.RemoveJob(jobId)
			}
		}
	case models.ChangeJobEvent:
		{ //修改一个任务事件
			logger.INFO("[#api#] ### %s, %+v", models.ChangeJobEvent, systemEvent)
			if len(systemEvent.JobIds) > 0 {
				job := cacheRepository.GetRawJob(systemEvent.JobIds[0])
				if job != nil {
					if job.Enabled == 1 {
						scheduler := request.Context.Get("Scheduler").(*scheduler.Scheduler)
						jobData := cacheRepository.GetAllocJob(job.Location, job.JobId)
						if jobData == nil {
							scheduler.SingleJobAlloc(job.Location, job.JobId) //重新加入分配表
						} else {
							if len(job.Servers) > 0 {
								scheduler.SingleJobAlloc(job.Location, job.JobId) //可能调整了servers, 需要重新分配一次.
							}
							cacheRepository.UpdateAllocJob(job.Location, job.JobId)
						}
					} else { //修改并关闭了任务
						cacheRepository.RemoveAllocJob(systemEvent.Runtime, job.JobId)
						cacheRepository.RemoveJob(job.JobId)
					}
				}
			}
		}
	case models.ChangeJobsFileEvent:
		{ //批量修改job任务文件
			logger.INFO("[#api#] ### %s, %+v", models.ChangeJobsFileEvent, systemEvent)
			for _, jobId := range systemEvent.JobIds {
				cacheRepository.GetRawJob(jobId)
			}
			cacheRepository.UpdateAllocJobs(systemEvent.Runtime, systemEvent.JobIds)
		}
	}
	return nil
}

//ProcessJobExecuteMessage is exported
func ProcessJobExecuteMessage(request *MessageRequest) error {

	jobExecute := &models.JobExecute{}
	if err := json.NewDecoder(request.Reader).Decode(jobExecute); err != nil {
		return fmt.Errorf("process jobexecute message %s failure, %s", request.Header.MsgId, err.Error())
	}

	logger.INFO("[#api#] process jobexecute message %s %s", jobExecute.JobId, jobExecute.Location)
	messageCache := request.Context.Get("MessageCache").(*models.MessageCache)
	if messageCache.ValidateMessage(jobExecute) {
		cacheRepository := request.Context.Get("CacheRepository").(*cache.CacheRepository)
		cacheRepository.SetJobExecute(jobExecute.JobId, jobExecute.State, jobExecute.ExecErr, jobExecute.ExecAt, jobExecute.NextAt)
	}
	return nil
}

//ProcessJobSelectMessage is exported
func ProcessJobSelectMessage(request *MessageRequest) error {

	jobSelect := &models.JobSelect{}
	if err := json.NewDecoder(request.Reader).Decode(jobSelect); err != nil {
		return fmt.Errorf("process jobselect message %s failure, %s", request.Header.MsgId, err.Error())
	}

	logger.INFO("[#api#] process jobselect message %s %s", jobSelect.JobId, jobSelect.Location)
	messageCache := request.Context.Get("MessageCache").(*models.MessageCache)
	if messageCache.ValidateMessage(jobSelect) {
		cacheRepository := request.Context.Get("CacheRepository").(*cache.CacheRepository)
		cacheRepository.SetJobNextAt(jobSelect.JobId, jobSelect.NextAt)
	}
	return nil
}
