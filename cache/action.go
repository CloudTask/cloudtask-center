package cache

import "github.com/cloudtask/libtools/gounits/httpx"

import (
	"context"
	"fmt"
	"net/http"
)

func putJobAction(client *httpx.HttpClient, rawurl string, location string, jobid string, action string) error {

	jobAction := struct {
		Runtime string `json:"runtime"`
		JobId   string `json:"jobid"`
		Action  string `json:"action"`
	}{
		Runtime: location,
		JobId:   jobid,
		Action:  action,
	}

	respData, err := client.PutJSON(context.Background(), rawurl+"/cloudtask/v2/jobs/action", nil, &jobAction, nil)
	if err != nil {
		return err
	}

	defer respData.Close()
	statusCode := respData.StatusCode()
	if statusCode >= http.StatusBadRequest {
		return fmt.Errorf("HTTP PUT job %s action %s failure %d.", jobid, action, statusCode)
	}
	return nil
}
