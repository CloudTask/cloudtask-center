package api

import "github.com/cloudtask/common/models"
import "github.com/gorilla/mux"

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"strings"
)

//ResolveJobBaseRequest is exported
func ResolveJobBaseRequest(c *Context) string {

	vars := mux.Vars(c.request)
	jobid := strings.TrimSpace(vars["jobid"])
	if len(jobid) == 0 {
		return ""
	}
	return jobid
}

//ResolveJobsAllocDataRequest is exported
func ResolveJobsAllocDataRequest(c *Context) string {

	vars := mux.Vars(c.request)
	runtime := strings.TrimSpace(vars["runtime"])
	if len(runtime) == 0 {
		return ""
	}
	return runtime
}

//ResolveServerJobsAllocDataRequest is exported
func ResolveServerJobsAllocDataRequest(c *Context) *ServerJobsAllocDataRequest {

	vars := mux.Vars(c.request)
	runtime := strings.TrimSpace(vars["runtime"])
	if len(runtime) == 0 {
		return nil
	}

	server := strings.TrimSpace(vars["server"])
	if len(server) == 0 {
		return nil
	}

	return &ServerJobsAllocDataRequest{
		Context: c,
		Runtime: runtime,
		Server:  server,
	}
}

//ResolveServersRequest is exported
func ResolveServersRequest(c *Context) string {

	vars := mux.Vars(c.request)
	runtime := strings.TrimSpace(vars["runtime"])
	if len(runtime) == 0 {
		return ""
	}
	return runtime
}

//ResolveJobActionRequest is exported
func ResolveJobActionRequest(c *Context) *JobActionRequest {

	buf, err := ioutil.ReadAll(c.request.Body)
	if err != nil {
		return nil
	}

	request := &JobActionRequest{}
	if err := json.NewDecoder(bytes.NewReader(buf)).Decode(request); err != nil {
		return nil
	}

	request.Context = c
	return request
}

//ResolveMessageRequest is exported
func ResolveMessageRequest(c *Context) *MessageRequest {

	buf, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return nil
	}

	msgHeader := &models.MsgHeader{}
	reader := bytes.NewReader(buf)
	if err := json.NewDecoder(reader).Decode(msgHeader); err != nil {
		return nil
	}

	if _, err := reader.Seek(0, 0); err != nil {
		return nil
	}

	return &MessageRequest{
		Header:  msgHeader,
		Reader:  reader,
		Context: c,
	}
}

//ResloveJogRequest is exported
func ResloveJogRequest(c *Context) *JobLogRequest {

	buf, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return nil
	}

	request := &JobLogRequest{}
	if err := json.NewDecoder(bytes.NewReader(buf)).Decode(request); err != nil {
		return nil
	}

	request.Context = c
	return request
}
