package ngcloud

import "github.com/cloudtask/cloudtask-center/cache/driver/types"
import "github.com/cloudtask/common/models"
import "github.com/cloudtask/libtools/gounits/httpx"

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

//Engine is exported
type Engine struct {
	rawAPIURL    string
	readPageSize int
	client       *httpx.HttpClient
}

//NewEngine is exported
func NewEngine(rawAPIURL string, readPageSize int) *Engine {

	client := httpx.NewClient().
		SetTransport(&http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
			DisableKeepAlives:     false,
			MaxIdleConns:          25,
			MaxIdleConnsPerHost:   25,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   http.DefaultTransport.(*http.Transport).TLSHandshakeTimeout,
			ExpectContinueTimeout: http.DefaultTransport.(*http.Transport).ExpectContinueTimeout,
		})

	return &Engine{
		rawAPIURL:    rawAPIURL,
		readPageSize: readPageSize,
		client:       client,
	}
}

func (engine *Engine) SetConfigParameters(rawAPIURL string, readPageSize int) {

	engine.rawAPIURL = rawAPIURL
	engine.readPageSize = readPageSize
}

func (engine *Engine) getLocation(location string) (*models.WorkLocation, error) {

	respData, err := engine.client.Get(context.Background(), engine.rawAPIURL+"/sys_locations/"+location, nil, nil)
	if err != nil {
		return nil, err
	}

	defer respData.Close()
	statusCode := respData.StatusCode()
	if statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("HTTP GET sys_locations %s failure %d.", location, statusCode)
	}

	if statusCode == http.StatusNoContent {
		return nil, types.ErrDriverResourceNotFound
	}

	workLocation := &models.WorkLocation{}
	if err := respData.JSON(workLocation); err != nil {
		return nil, err
	}
	return workLocation, nil
}

func (engine *Engine) postLocation(workLocation *models.WorkLocation) error {

	respData, err := engine.client.PostJSON(context.Background(), engine.rawAPIURL+"/sys_locations", nil, workLocation, nil)
	if err != nil {
		return err
	}

	defer respData.Close()
	statusCode := respData.StatusCode()
	if statusCode >= http.StatusBadRequest {
		return fmt.Errorf("HTTP POST sys_locations %s failure %d.", workLocation.Location, statusCode)
	}
	return nil
}

func (engine *Engine) readLocationsName() ([]string, error) {

	var (
		pageIndex = 1
		names     = []string{}
	)

	query := map[string][]string{
		"pageSize": []string{strconv.Itoa(engine.readPageSize)},
		"fields":   []string{`["location"]`},
	}

	for {
		query["pageIndex"] = []string{strconv.Itoa(pageIndex)}
		respData, err := engine.client.Get(context.Background(), engine.rawAPIURL+"/sys_locations", query, nil)
		if err != nil {
			return nil, err
		}

		defer respData.Close()
		statusCode := respData.StatusCode()
		if statusCode >= http.StatusBadRequest {
			return nil, fmt.Errorf("HTTP GET sys_locations names failure %d.", statusCode)
		}

		respLocationsName, err := parseLocationsNameResponse(respData)
		if err != nil {
			return nil, err
		}

		names = append(names, respLocationsName.Names...)
		if len(names) != respLocationsName.TotalRows {
			pageIndex++
			continue
		}
		break
	}
	return names, nil
}

func (engine *Engine) readSimpleJobs(query map[string][]string) ([]*models.SimpleJob, error) {

	var (
		pageIndex = 1
		jobs      = []*models.SimpleJob{}
	)

	query["pageSize"] = []string{strconv.Itoa(engine.readPageSize)}
	query["fields"] = []string{`["jobid","name","location","groupid","servers","enabled","stat"]`}
	for {
		query["pageIndex"] = []string{strconv.Itoa(pageIndex)}
		respData, err := engine.client.Get(context.Background(), engine.rawAPIURL+"/sys_jobs", query, nil)
		if err != nil {
			return nil, err
		}

		defer respData.Close()
		statusCode := respData.StatusCode()
		if statusCode >= http.StatusBadRequest {
			return nil, fmt.Errorf("HTTP GET sys_jobs jobs failure %d.", statusCode)
		}

		respSimpleJobs, err := parseSimpleJobsResponse(respData)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, respSimpleJobs.Jobs...)
		if len(jobs) != respSimpleJobs.TotalRows {
			pageIndex++
			continue
		}
		break
	}
	return jobs, nil
}

func (engine *Engine) readJobs(query map[string][]string) ([]*models.Job, error) {

	var (
		pageIndex = 1
		jobs      = []*models.Job{}
	)

	query["pageSize"] = []string{strconv.Itoa(engine.readPageSize)}
	for {
		query["pageIndex"] = []string{strconv.Itoa(pageIndex)}
		respData, err := engine.client.Get(context.Background(), engine.rawAPIURL+"/sys_jobs", query, nil)
		if err != nil {
			return nil, err
		}

		defer respData.Close()
		statusCode := respData.StatusCode()
		if statusCode >= http.StatusBadRequest {
			return nil, fmt.Errorf("HTTP GET sys_jobs jobs failure %d.", statusCode)
		}

		respJobs, err := parseJobsResponse(respData)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, respJobs.Jobs...)
		if len(jobs) != respJobs.TotalRows {
			pageIndex++
			continue
		}
		break
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

	respData, err := engine.client.Get(context.Background(), engine.rawAPIURL+"/sys_jobs/"+jobid, nil, nil)
	if err != nil {
		return nil, err
	}

	defer respData.Close()
	statusCode := respData.StatusCode()
	if statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("HTTP GET sys_jobs %s failure %d.", jobid, statusCode)
	}

	if statusCode == http.StatusNoContent {
		return nil, types.ErrDriverResourceNotFound
	}

	job := &models.Job{}
	if err := respData.JSON(job); err != nil {
		return nil, err
	}
	return job, nil
}

func (engine *Engine) putJob(job *models.Job) error {

	respData, err := engine.client.PutJSON(context.Background(), engine.rawAPIURL+"/sys_jobs", nil, job, nil)
	if err != nil {
		return err
	}

	defer respData.Close()
	statusCode := respData.StatusCode()
	if statusCode >= http.StatusBadRequest {
		return fmt.Errorf("HTTP PUT sys_jobs %s failure %d.", job.JobId, statusCode)
	}
	return nil
}

func (engine *Engine) postJobLog(jobLog *models.JobLog) error {

	query := map[string][]string{
		"strict": []string{"true"},
	}

	resp, err := engine.client.PostJSON(context.Background(), engine.rawAPIURL+"/logs", query, jobLog, nil)
	if err != nil {
		return err
	}

	defer resp.Close()
	statusCode := resp.StatusCode()
	if statusCode >= http.StatusBadRequest {
		return fmt.Errorf("HTTP POST logs %s failure %d.", jobLog.JobId, statusCode)
	}
	return nil
}
