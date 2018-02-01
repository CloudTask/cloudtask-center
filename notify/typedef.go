package notify

//EndPoint is exported
type EndPoint struct {
	Name     string `yaml:"name"`
	URL      string `yaml:"url"`
	Enabled  bool   `yaml:"enabled"`
	Sender   string `yaml:"sender"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

//Notifications is exported
type Notifications struct {
	EndPoints []EndPoint `yaml:"endpoints,omitempty"`
}

//Server is exported
type Server struct {
	IP    string
	Name  string
	State string
}

//WatchLocation is exported
type WatchLocation struct {
	Name        string
	ContactInfo []string
	Servers     []*Server
}

//WatchLocations is exported
type WatchLocations map[string]*WatchLocation

//AddServer is exported
func (location *WatchLocation) AddServer(ip string, name string, state string) {

	for _, server := range location.Servers {
		if server.IP == ip || server.Name == name {
			return
		}
	}

	location.Servers = append(location.Servers, &Server{
		IP:    ip,
		Name:  name,
		State: state,
	})
}

//AddContactInfo is exported
func (location *WatchLocation) AddContactInfo(value string) {

	for _, contactname := range location.ContactInfo {
		if contactname == value {
			return
		}
	}
	location.ContactInfo = append(location.ContactInfo, value)
}

//JobResult is exported
type JobResult struct {
	JobName    string
	Location   string
	Server     string
	Execat     string
	IsSuccessd bool
	Content    string
	Stdout     string
	Errout     string
	Execerr    string
}

//WatchJobNotify is exported
type WatchJobNotify struct {
	Name        string
	ContactInfo []string
	JobResult
}
