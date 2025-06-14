package task

import "github.com/robfig/cron/v3"

func Start() {
	c := cron.New(
		//如果前一个还在运行，就直接跳过本次调度。
		cron.WithChain(
			cron.SkipIfStillRunning(cron.DefaultLogger),
		),
	)
	// 汇率监听
	c.AddJob("@every 60s", UsdtRateJob{})
	// trc20钱包监听
	c.AddJob("@every 15s", ListenTrc20Job{})
	// polygon钱包监听
	c.AddJob("@every 15s", ListenPolygonJob{})
	c.Start()
}
