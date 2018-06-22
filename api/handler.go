package api

import "github.com/cloudtask/cloudtask-center/cache"
import "github.com/cloudtask/cloudtask-center/notify"
import "github.com/cloudtask/common/models"

import (
	"crypto/md5"
	"encoding/hex"
	"net/http"
)

func getJobBase(c *Context) error {

	response := &ResponseImpl{}
	jobid := ResolveJobBaseRequest(c)
	if jobid == "" {
		response.SetContent(ErrRequestResolveInvaild.Error())
		return c.JSON(http.StatusBadRequest, response)
	}

	cacheRepository := c.Get("CacheRepository").(*cache.CacheRepository)
	job := cacheRepository.GetJob(jobid)
	if job == nil {
		response.SetContent(ErrRequestNotFound.Error())
		return c.JSON(http.StatusNotFound, response)
	}

	version := 0
	if jobData := cacheRepository.GetAllocJob(job.Location, job.JobId); jobData != nil {
		version = jobData.Version
	}

	encoder := md5.New() //根据文件名计算filecode(md5)
	encoder.Write([]byte(job.FileName))
	fileCode := hex.EncodeToString(encoder.Sum(nil))
	jobBase := &models.JobBase{
		JobId:    job.JobId,
		JobName:  job.Name,
		FileName: job.FileName,
		FileCode: fileCode,
		Cmd:      job.Cmd,
		Env:      job.Env,
		Timeout:  job.Timeout,
		Version:  version,
		Schedule: job.Schedule,
	}
	return c.JSON(http.StatusOK, jobBase)
}

func getJobsAllocData(c *Context) error {

	response := &ResponseImpl{}
	runtime := ResolveJobsAllocDataRequest(c)
	if runtime == "" {
		response.SetContent(ErrRequestResolveInvaild.Error())
		return c.JSON(http.StatusBadRequest, response)
	}

	cacheRepository := c.Get("CacheRepository").(*cache.CacheRepository)
	jobsAllocData := cacheRepository.GetAllocData(runtime)
	if jobsAllocData == nil {
		response.SetContent(ErrRequestNotFound.Error())
		return c.JSON(http.StatusNotFound, response)
	}

	respData := GetJobsAllocDataResponse{JobsAlloc: jobsAllocData}
	response.SetContent(ErrRequestSuccessed.Error())
	response.SetData(respData)
	return c.JSON(http.StatusOK, response)
}

func getServerJobsAllocData(c *Context) error {

	response := &ResponseImpl{}
	request := ResolveServerJobsAllocDataRequest(c)
	if request == nil {
		response.SetContent(ErrRequestResolveInvaild.Error())
		return c.JSON(http.StatusBadRequest, response)
	}

	cacheRepository := c.Get("CacheRepository").(*cache.CacheRepository)
	jobsAllocData := cacheRepository.GetServerAllocData(request.Runtime, request.Server)
	if jobsAllocData == nil {
		response.SetContent(ErrRequestNotFound.Error())
		return c.JSON(http.StatusNotFound, response)
	}

	respData := GetJobsAllocDataResponse{JobsAlloc: jobsAllocData}
	response.SetContent(ErrRequestSuccessed.Error())
	response.SetData(respData)
	return c.JSON(http.StatusOK, response)
}

func getServers(c *Context) error {

	response := &ResponseImpl{}
	runtime := ResolveServersRequest(c)
	if runtime == "" {
		response.SetContent(ErrRequestResolveInvaild.Error())
		return c.JSON(http.StatusBadRequest, response)
	}

	cacheRepository := c.Get("CacheRepository").(*cache.CacheRepository)
	workers := cacheRepository.GetWorkers(runtime)
	respData := GetServersResponse{Servers: []*models.Server{}}
	for _, worker := range workers {
		respData.Servers = append(respData.Servers, worker.Server)
	}
	response.SetContent(ErrRequestSuccessed.Error())
	response.SetData(respData)
	return c.JSON(http.StatusOK, response)
}

func postMessages(c *Context) error {

	response := &ResponseImpl{}
	request := ResolveMessageRequest(c)
	if request == nil {
		response.SetContent(ErrRequestResolveInvaild.Error())
		return c.JSON(http.StatusBadRequest, response)
	}

	switch request.Header.MsgName {
	case models.MsgSystemEvent:
		{
			if err := ProcessSystemEventMessage(request); err != nil {
				response.SetContent(err.Error())
				return c.JSON(http.StatusInternalServerError, response)
			}
		}
	case models.MsgJobExecute:
		{
			if err := ProcessJobExecuteMessage(request); err != nil {
				response.SetContent(err.Error())
				return c.JSON(http.StatusInternalServerError, response)
			}
		}
	case models.MsgJobSelect:
		{
			if err := ProcessJobSelectMessage(request); err != nil {
				response.SetContent(err.Error())
				return c.JSON(http.StatusInternalServerError, response)
			}
		}
	}
	response.SetContent(ErrRequestAccepted.Error())
	return c.JSON(http.StatusAccepted, response)
}

func postLogs(c *Context) error {

	response := &ResponseImpl{}
	request := ResloveJogRequest(c)
	if request == nil {
		response.SetContent(ErrRequestResolveInvaild.Error())
		return c.JSON(http.StatusBadRequest, response)
	}
	cacheRepository := c.Get("CacheRepository").(*cache.CacheRepository)
	cacheRepository.SetJobLog(&request.JobLog)
	job := cacheRepository.GetJob(request.JobId)
	if job != nil && job.NotifySetting != nil {
		var notifyOpt *models.Notify
		var isSuccessd bool
		switch request.JobLog.Stat {
		case models.STATE_STOPED:
			notifyOpt = &job.NotifySetting.Succeed
			isSuccessd = true
		case models.STATE_FAILED:
			notifyOpt = &job.NotifySetting.Failed
			isSuccessd = false
		}

		if notifyOpt != nil && notifyOpt.Enabled {
			var execAt string
			if !request.JobLog.ExecAt.IsZero() {
				execAt = request.JobLog.ExecAt.String()
			}
			watchJobNotify := &notify.WatchJobNotify{
				ContactInfo: []string{notifyOpt.To},
				JobResult: notify.JobResult{
					JobName:    job.Name,
					Directory:  request.JobLog.WorkDir,
					Location:   job.Location,
					Server:     request.JobLog.IpAddr,
					Execat:     execAt,
					IsSuccessd: isSuccessd,
					Content:    notifyOpt.Content,
					Stdout:     request.JobLog.StdOut,
					Errout:     request.JobLog.ErrOut,
					Execerr:    request.JobLog.ExecErr,
				},
			}
			notifySender := c.Get("NotifySender").(*notify.NotifySender)
			notifySender.AddJobNotifyEvent("This Job Has Been Executed.", watchJobNotify)
		}
	}
	response.SetContent(ErrRequestAccepted.Error())
	return c.JSON(http.StatusAccepted, response)
}

func putJobAction(c *Context) error {

	response := &ResponseImpl{}
	request := ResolveJobActionRequest(c)
	if request == nil {
		response.SetContent(ErrRequestResolveInvaild.Error())
		return c.JSON(http.StatusBadRequest, response)
	}

	cacheRepository := c.Get("CacheRepository").(*cache.CacheRepository)
	cacheRepository.SetJobAction(request.Runtime, request.JobId, request.Action)
	response.SetContent(ErrRequestAccepted.Error())
	return c.JSON(http.StatusAccepted, response)
}
