package notify

import "github.com/cloudtask/libtools/gounits/logger"
import "github.com/cloudtask/libtools/gounits/httpx"

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"
)

// APIEndPoint is exported
type APIEndPoint struct {
	IEndPoint
	EndPoint
	client *httpx.HttpClient
}

// NewAPIEndPoint is exported
func NewAPIEndPoint(endpoint EndPoint) IEndPoint {

	client := httpx.NewClient().
		SetTransport(&http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
			DisableKeepAlives:     false,
			MaxIdleConns:          10,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       60 * time.Second,
			TLSHandshakeTimeout:   http.DefaultTransport.(*http.Transport).TLSHandshakeTimeout,
			ExpectContinueTimeout: http.DefaultTransport.(*http.Transport).ExpectContinueTimeout,
		})

	return &APIEndPoint{
		EndPoint: endpoint,
		client:   client,
	}
}

// DoEvent is exported
func (endpoint *APIEndPoint) DoEvent(event *Event, data interface{}) {

	if !endpoint.Enabled {
		return
	}

	value := map[string]interface{}{
		"From":        endpoint.Sender,
		"To":          strings.Join(event.ContactInfo, ";"),
		"Subject":     event.makeSubjectText(),
		"Body":        data,
		"ContentType": "HTML",
		"MailType":    "Smtp",
		"SmtpSetting": map[string]interface{}{},
	}

	resp, err := endpoint.client.PostJSON(context.Background(), endpoint.URL, nil, value, nil)
	if err != nil {
		logger.ERROR("[#notify#] api endpoint error: %s", err.Error())
		return
	}
	resp.Close()
}
