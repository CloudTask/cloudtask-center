package types

import "errors"

type Backend string

const (
	//MONGO, mongodb driver.
	MONGO Backend = "mongo"
	//NGCLOUD, newegg clouddata driver.
	NGCLOUD Backend = "ngcloud"
)

var (
	ErrDriverNotSupported     = errors.New("storage not supported yet, please choose one of")
	ErrDriverResourceNotFound = errors.New("storage resource not found.")
)

//Parameters is exported
//config optation key-value pairs
type Parameters map[string]interface{}

//StorageDriverConfigs is exported
//driver configs.
type StorageDriverConfigs map[string]Parameters
