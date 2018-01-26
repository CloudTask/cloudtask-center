package notify

import (
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

//notify template string
var templateBody string

//NotifySender is exported
type NotifySender struct {
	sync.RWMutex
	initWatch bool
	endPoints []IEndPoint
	events    map[string]*Event
}

//NewNotifySender is exported
func NewNotifySender(endPoints []EndPoint) *NotifySender {

	sender := &NotifySender{
		initWatch: true,
		endPoints: []IEndPoint{},
		events:    make(map[string]*Event),
	}

	if buf, err := ioutil.ReadFile("./notify/template.html"); err == nil {
		templateBody = string(buf)
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
				wgroup.Add(1)
				go func(e *Event) {
					e.dispatch(templateBody)
					wgroup.Done()
				}(event)
			}
			wgroup.Wait()
		}
		for _, event := range sender.events {
			delete(sender.events, event.ID)
		}
	}
	sender.Unlock()
}
