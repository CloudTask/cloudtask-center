package scheduler

func (s *Scheduler) PrefAlloc(location string, nodes *Nodes) {

	//目前配置主要使用hash分配算法,性能方式分配算法暂未实现，不影响项目使用，后期陆续完成.
	//思路：
	//1、worker心跳包中根据配置每(15m)带一次内存占用信息，job执行开销评估数据.
	//2、server结合心跳数据，定期检查worker的job数，按job数与执行开销均分job.
	//3、server评估下线率比较大的worker，加入黑名单，尽量不把job分配到该worker上.
}

func (s *Scheduler) PrefSingleJobAlloc(location string, jobid string) {
}
