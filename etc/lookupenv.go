package etc

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

//ParseEnv is exported
func (conf *Configuration) parseEnv() error {

	pidFile := os.Getenv("CLOUDTASK_PIDFILE")
	if pidFile != "" {
		conf.PidFile = pidFile
	}

	retryStartup := os.Getenv("CLOUDTASK_RETRYSTARTUP")
	if retryStartup != "" {
		value, err := strconv.ParseBool(retryStartup)
		if err != nil {
			return fmt.Errorf("CLOUDTASK_RETRYSTARTUP invalid, %s", err.Error())
		}
		conf.RetryStartup = value
	}

	useServerConfig := os.Getenv("CLOUDTASK_USESERVERCONFIG")
	if useServerConfig != "" {
		value, err := strconv.ParseBool(useServerConfig)
		if err != nil {
			return fmt.Errorf("CLOUDTASK_USESERVERCONFIG invalid, %s", err.Error())
		}
		conf.UseServerConfig = value
	}

	var err error
	//parse cluster env
	if err = parseClusterEnv(conf); err != nil {
		return err
	}

	//parse API env
	if err = parseAPIEnv(conf); err != nil {
		return err
	}

	//parse scheduler env
	if err = parseSchedulerEnv(conf); err != nil {
		return err
	}

	//parse cache env
	if err = parseCacheEnv(conf); err != nil {
		return err
	}

	//parse logger env
	if err = parseLoggerEnv(conf); err != nil {
		return err
	}
	return nil
}

func parseClusterEnv(conf *Configuration) error {

	if clusterHosts := os.Getenv("CLOUDTASK_CLUSTER_HOSTS"); clusterHosts != "" {
		conf.Cluster.Hosts = clusterHosts
	}

	if clusterName := os.Getenv("CLOUDTASK_CLUSTER_NAME"); clusterName != "" {
		if ret := filepath.HasPrefix(clusterName, "/"); !ret {
			clusterName = "/" + clusterName
		}
		conf.Cluster.Root = clusterName
	}

	if clusterPulse := os.Getenv("CLOUDTASK_CLUSTER_PULSE"); clusterPulse != "" {
		if _, err := time.ParseDuration(clusterPulse); err != nil {
			return fmt.Errorf("CLOUDTASK_CLUSTER_PULSE invalid, %s", err.Error())
		}
		conf.Cluster.Pulse = clusterPulse
	}

	if clusterTimeout := os.Getenv("CLOUDTASK_CLUSTER_TIMEOUT"); clusterTimeout != "" {
		if _, err := time.ParseDuration(clusterTimeout); err != nil {
			return fmt.Errorf("CLOUDTASK_CLUSTER_TIMEOUT invalid, %s", err.Error())
		}
		conf.Cluster.Timeout = clusterTimeout
	}

	if clusterThreshold := os.Getenv("CLOUDTASK_CLUSTER_THRESHOLD"); clusterThreshold != "" {
		value, err := strconv.Atoi(clusterThreshold)
		if err != nil {
			return fmt.Errorf("CLOUDTASK_CLUSTER_THRESHOLD invalid, %s", err.Error())
		}
		conf.Cluster.Threshold = value
	}
	return nil
}

func parseAPIEnv(conf *Configuration) error {

	if apiHost := os.Getenv("CLOUDTASK_API_HOST"); apiHost != "" {
		hostIP, hostPort, err := net.SplitHostPort(apiHost)
		if err != nil {
			return fmt.Errorf("CLOUDTASK_API_HOST invalid, %s", err.Error())
		}
		if hostIP != "" {
			if _, err := net.LookupHost(hostIP); err != nil {
				return fmt.Errorf("CLOUDTASK_API_HOST invalid, %s", err.Error())
			}
		}
		conf.API.Hosts = []string{net.JoinHostPort(hostIP, hostPort)}
	}

	if enableCors := os.Getenv("CLOUDTASK_API_ENABLECORS"); enableCors != "" {
		value, err := strconv.ParseBool(enableCors)
		if err != nil {
			return fmt.Errorf("CLOUDTASK_API_ENABLECORS invalid, %s", err.Error())
		}
		conf.API.EnableCors = value
	}
	return nil
}

func parseSchedulerEnv(conf *Configuration) error {

	if schedulerAllocMode := os.Getenv("CLOUDTASK_SCHEDULER_ALLOCMODE"); schedulerAllocMode != "" {
		conf.Scheduler.AllocMode = schedulerAllocMode
	}

	if schedulerAllocRecovery := os.Getenv("CLOUDTASK_SCHEDULER_ALLOCRECOVERY"); schedulerAllocRecovery != "" {
		if _, err := time.ParseDuration(schedulerAllocRecovery); err != nil {
			return fmt.Errorf("CLOUDTASK_SCHEDULER_ALLOCRECOVERY invalid, %s", err.Error())
		}
		conf.Scheduler.AllocRecovery = schedulerAllocRecovery
	}
	return nil
}

func parseCacheEnv(conf *Configuration) error {

	if cacheLRUSize := os.Getenv("CLOUDTASK_CACHE_LRUSIZE"); cacheLRUSize != "" {
		value, err := strconv.Atoi(cacheLRUSize)
		if err != nil {
			return fmt.Errorf("CLOUDTASK_CACHE_LRUSIZE invalid, %s", err.Error())
		}
		conf.Cache.LRUSize = value
	}
	return nil
}

func parseLoggerEnv(conf *Configuration) error {

	if logFile := os.Getenv("CLOUDTASK_LOG_FILE"); logFile != "" {
		conf.Logger.LogFile = logFile
	}

	if logLevel := os.Getenv("CLOUDTASK_LOG_LEVEL"); logLevel != "" {
		conf.Logger.LogLevel = logLevel
	}

	if logSize := os.Getenv("CLOUDTASK_LOG_SIZE"); logSize != "" {
		value, err := strconv.ParseInt(logSize, 10, 64)
		if err != nil {
			return fmt.Errorf("CLOUDTASK_LOG_SIZE invalid, %s", err.Error())
		}
		conf.Logger.LogSize = value
	}
	return nil
}
