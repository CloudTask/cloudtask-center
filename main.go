package main

import "github.com/cloudtask/libtools/gounits/system"

import (
	"log"
	"os"
)

func main() {

	jobserver, err := NewJobServer()
	if err != nil {
		log.Printf("jobserver error, %s\n", err.Error())
		os.Exit(system.ErrorExitCode(err))
	}

	defer func() {
		exitCode := 0
		if err := jobserver.Stop(); err != nil {
			log.Printf("jobserver stop error, %s\n", err.Error())
			exitCode = system.ErrorExitCode(err)
		}
		os.Exit(exitCode)
	}()

	if err = jobserver.Startup(); err != nil {
		log.Printf("jobserver startup error, %s\n", err.Error())
		os.Exit(system.ErrorExitCode(err))
	}
	system.InitSignal(nil)
}
