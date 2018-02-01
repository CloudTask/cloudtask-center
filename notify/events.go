package notify

import "github.com/cloudtask/libtools/gounits/rand"

import (
	"bytes"
	"html/template"
	"time"
)

//EventType is exported
type EventType int

const (
	//LocationServersEvent is exported
	//cluster discovery watch servers event
	LocationServersEvent EventType = 1000
	//JobNotifyEvent is exported
	//job executed notify event
	JobNotifyEvent EventType = 1001
)

//eventsTextMap is exported
var eventsTextMap = map[EventType]string{
	LocationServersEvent: "LocationServersEvent",
	JobNotifyEvent:       "JobNotifyEvent",
}

//Event is exported
type Event struct {
	ID          string
	Type        EventType
	Name        string
	Error       error
	ContactInfo []string
	Endpoints   []IEndPoint
	data        map[string]interface{}
}

//NewEvent is exported
func NewEvent(eventType EventType, description string, err error, contactInfo []string, endpoints []IEndPoint) *Event {

	seed := time.Now()
	event := &Event{
		ID:          rand.UUID(true),
		Type:        eventType,
		Name:        eventsTextMap[eventType],
		Error:       err,
		ContactInfo: contactInfo,
		Endpoints:   endpoints,
	}

	event.data = map[string]interface{}{
		"ID":          event.ID,
		"Event":       event.Name,
		"Description": description,
		"Timestamp":   seed.UnixNano(),
		"Datetime":    seed,
	}

	if err != nil {
		event.data["Exception"] = err.Error()
	}
	return event
}

//Dispatch is exported
func (event *Event) dispatch(templateBody string) {

	if len(templateBody) > 0 {
		var buf bytes.Buffer
		t := template.New("")
		t.Parse(templateBody)
		t.Execute(&buf, event.data)
		for _, endPoint := range event.Endpoints {
			endPoint.DoEvent(event, buf.String())
		}
	}
}

//makeSubjectText is exported
func (event *Event) makeSubjectText() string {

	subjectPrefix := "(info)"
	if event.Error != nil {
		subjectPrefix = "(error)"
	}
	return subjectPrefix + " CloudTask Notification"
}
