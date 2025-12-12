package task

import "github.com/robfig/cron/v3"

func Start() {
	c := cron.New(
		//如果前一个还在运行，就直接跳过本次调度。
		cron.WithChain(
			cron.SkipIfStillRunning(cron.DefaultLogger),
		),
	)
	c.AddJob("@every 60s", UsdtRateJob{})
	c.AddJob("@every 15s", ListenTrc20Job{})
	c.AddJob("@every 15s", ListenEvmJob{})
	c.AddJob("@every 15s", ListenAptosJob{})
	c.Start()
}
