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

//JobActionRequest is exported
type JobActionRequest struct {
	Context *Context
	Runtime string `json:"runtime"`
	JobId   string `json:"jobid"`
	Action  string `json:"action"`
}
