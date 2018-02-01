package api

import "github.com/cloudtask/cloudtask-center/etc"
import "github.com/cloudtask/libtools/gzkwrapper"
import "github.com/gorilla/mux"

import (
	"net/http"
)

type handler func(c *Context) error

var routes = map[string]map[string]handler{
	"GET": {
		"/cloudtask/v2/_ping":                        ping,
		"/cloudtask/v2/jobs/{jobid}/base":            getJobBase,
		"/cloudtask/v2/runtimes/{runtime}/jobsalloc": getJobsAllocData,
		"/cloudtask/v2/runtimes/{runtime}/servers":   getServers,
	},
	"POST": {
		"/cloudtask/v2/messages": postMessages,
		"/cloudtask/v2/logs":     postLogs,
	},
	"PUT": {
		"/cloudtask/v2/jobs/action": putJobAction,
	},
	"DELETE": {
		"/cloudtask/v2/runtimes/{runtime}": deleteRuntime,
	},
}

func NewRouter(enableCors bool, store Store) *mux.Router {

	router := mux.NewRouter()
	for method, mappings := range routes {
		for route, handler := range mappings {
			routemethod := method
			routepattern := route
			routehandler := handler
			wrap := func(w http.ResponseWriter, r *http.Request) {
				if enableCors {
					writeCorsHeaders(w, r)
				}
				c := NewContext(w, r, store)
				routehandler(c)
			}
			router.Path(routepattern).Methods(routemethod).HandlerFunc(wrap)
			if enableCors {
				optionsmethod := "OPTIONS"
				optionshandler := optionsHandler
				wrap := func(w http.ResponseWriter, r *http.Request) {
					if enableCors {
						writeCorsHeaders(w, r)
					}
					c := NewContext(w, r, store)
					optionshandler(c)
				}
				router.Path(routepattern).Methods(optionsmethod).HandlerFunc(wrap)
			}
		}
	}
	return router
}

func ping(c *Context) error {

	pangData := struct {
		AppCode      string               `json:"app"`
		ServerKey    string               `json:"key"`
		NodeData     *gzkwrapper.NodeData `json:"node"`
		SystemConfig *etc.Configuration   `json:"systemconfig"`
	}{
		AppCode:      c.Get("AppCode").(string),
		ServerKey:    c.Get("ServerKey").(string),
		NodeData:     c.Get("NodeData").(*gzkwrapper.NodeData),
		SystemConfig: c.Get("SystemConfig").(*etc.Configuration),
	}
	return c.JSON(http.StatusOK, pangData)
}

func optionsHandler(c *Context) error {

	c.WriteHeader(http.StatusOK)
	return nil
}
