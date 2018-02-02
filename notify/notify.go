package notify

import (
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

//notify template string
var (
	LocationServersNotifyBody string
	JobResultNotifyBody       string
)

//NotifySender is exported
type NotifySender struct {
	sync.RWMutex
	initWatch bool
	endPoints []IEndPoint
	events    map[string]*Event
}

func init() {

	var (
		buf []byte
		err error
	)

	if buf, err = ioutil.ReadFile("./notify/templates/location.html"); err == nil {
		LocationServersNotifyBody = string(buf)
	}

	if buf, err = ioutil.ReadFile("./notify/templates/job.html"); err == nil {
		JobResultNotifyBody = string(buf)
	}
}

//NewNotifySender is exported
func NewNotifySender(endPoints []EndPoint) *NotifySender {

	sender := &NotifySender{
		initWatch: true,
		endPoints: []IEndPoint{},
		events:    make(map[string]*Event),
	}

	factory := &NotifyEndPointFactory{}
	sender.Lock()
	for _, endPoint := range endPoints {
		switch strings.ToUpper(endPoint.Name) {
		case "API":
			apiEndPoint := factory.CreateAPIEndPoint(endPoint)
			sender.endPoints = append(sender.endPoints, apiEndPoint)
		case "SMTP":
			smtpEndPoint := factory.CreateSMTPEndPoint(endPoint)
			sender.endPoints = append(sender.endPoints, smtpEndPoint)
		}
	}
	sender.Unlock()

	go func() {
		time.Sleep(60 * time.Second)
		sender.initWatch = false
	}()
	return sender
}

//AddLocationServersEvent is exported
func (sender *NotifySender) AddLocationServersEvent(description string, watchLocation *WatchLocation) {

	event := NewEvent(LocationServersEvent, description, nil, watchLocation.ContactInfo, sender.endPoints)
	event.data["WatchLocation"] = watchLocation
	sender.Lock()
	sender.events[event.ID] = event
	go sender.dispatchEvents()
	sender.Unlock()
}

//AddJobNotifyEvent is exported
func (sender *NotifySender) AddJobNotifyEvent(description string, watchJobNotify *WatchJobNotify) {

	event := NewEvent(JobNotifyEvent, description, nil, watchJobNotify.ContactInfo, sender.endPoints)
	event.data["WatchJobNotify"] = watchJobNotify
	sender.Lock()
	sender.events[event.ID] = event
	go sender.dispatchEvents()
	sender.Unlock()
}

//dispatchEvents is exported
//dispatch all events.
func (sender *NotifySender) dispatchEvents() {

	sender.Lock()
	for {
		if len(sender.events) == 0 {
			break
		}
		if !sender.initWatch {
			wgroup := sync.WaitGroup{}
			for _, event := range sender.events {
				if len(event.ContactInfo) > 0 {
					wgroup.Add(1)
					go func(e *Event) {
						templateBody := getNotifyTemplateBody(e.Type)
						if templateBody != "" {
							e.dispatch(templateBody)
						}
						wgroup.Done()
					}(event)
				}
			}
			wgroup.Wait()
		}
		for _, event := range sender.events {
			delete(sender.events, event.ID)
		}
	}
	sender.Unlock()
}

func getNotifyTemplateBody(evt EventType) string {

	if evt == LocationServersEvent {
		return LocationServersNotifyBody
	} else if evt == JobNotifyEvent {
		return JobResultNotifyBody
	}
	return ""
}
