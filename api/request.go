package api

import "github.com/cloudtask/common/models"

import (
	"bytes"
)

//MessageRequest is exported
type MessageRequest struct {
	Header  *models.MsgHeader
	Reader  *bytes.Reader
	Context *Context
}

//JobLogRequest is exported
type JobLogRequest struct {
	Context *Context
	models.JobLog
}

//JobActionRequest is exported
type JobActionRequest struct {
	Context *Context
	Runtime string `json:"runtime"`
	JobId   string `json:"jobid"`
	Action  string `json:"action"`
}

//ServerJobsAllocDataRequest is exported
type ServerJobsAllocDataRequest struct {
	Context *Context
	Runtime string `json:"runtime"`
	Server  string `json:"server"`
}
