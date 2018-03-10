package cache

import "github.com/cloudtask/common/models"

import (
	"strings"
)

func containsServer(it *models.Server, servers []*models.Server) bool {

	if it != nil {
		itIpAddr := strings.TrimSpace(it.IPAddr)
		itName := strings.TrimSpace(it.Name)
		for _, server := range servers {
			if ipAddr := strings.TrimSpace(server.IPAddr); ipAddr != "" {
				if itIpAddr == ipAddr {
					return true
				}
			}
			if hostName := strings.TrimSpace(server.Name); hostName != "" {
				if strings.ToUpper(hostName) == strings.ToUpper(itName) {
					return true
				}
			}
		}
	}
	return false
}
