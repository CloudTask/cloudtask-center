package cache

type ICacheRepositoryHandler interface {
	OnAllocCacheChangedHandlerFunc(location string, data []byte, err error)
	OnAllocCacheLocationRemovedHandlerFunc(location string)
	OnNodeCacheChangedHandlerFunc(event EventType, location string, worker *Worker)
	OnNodeCacheLocationRemovedHandlerFunc(location string)
}

type AllocCacheChangedHandlerFunc func(location string, data []byte, err error)

func (fn AllocCacheChangedHandlerFunc) OnAllocCacheChangedHandlerFunc(location string, data []byte, err error) {
	fn(location, data, err)
}

type AllocCacheLocationRemovedHandlerFunc func(location string)

func (fn AllocCacheLocationRemovedHandlerFunc) OnAllocCacheLocationRemovedHandlerFunc(location string) {
	fn(location)
}

type NodeCacheChangedHandlerFunc func(event EventType, location string, worker *Worker)

func (fn NodeCacheChangedHandlerFunc) OnNodeCacheChangedHandlerFunc(event EventType, location string, worker *Worker) {
	fn(event, location, worker)
}

type NodeCacheLocationRemovedHandlerFunc func(location string)

func (fn NodeCacheLocationRemovedHandlerFunc) OnNodeCacheLocationRemovedHandlerFunc(location string) {
	fn(location)
}
