package timewheel

import "time"

var tw = New(time.Second, 3600)

func init() {
	tw.start()
}

// Delay 在duration时间后执行任务
func Delay(duration time.Duration, key string, job func()) {
	tw.AddJob(duration, key, job)
}

// At 在指定时间点at执行任务
func At(at time.Time, key string, job func()) {
	tw.AddJob(at.Sub(time.Now()), key, job)
}

// Cancel 取消键值为key的定时任务
func Cancel(key string) {
	tw.RemoveJob(key)
}
