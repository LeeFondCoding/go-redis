package timewheel

import (
	"container/list"
	"goredis/lib/logger"
	"time"
)

type location struct {
	slot     int
	elemTask *list.Element
}

// TimeWheel 线程安全的时间轮
type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List

	timer        map[string]*location
	curPos       int
	slotNum      int
	addTaskChan  chan *task
	removeTaskCh chan string
	stopChan     chan struct{}
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:     interval,
		slots:        make([]*list.List, slotNum),
		timer:        make(map[string]*location),
		curPos:       0,
		slotNum:      slotNum,
		addTaskChan:  make(chan *task),
		removeTaskCh: make(chan string),
		stopChan:     make(chan struct{}),
	}
	tw.initSlots()
	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := range tw.slots {
		tw.slots[i] = list.New()
	}
}

// Start 启动时间轮, 后台开启守护goroutine
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop 关闭后台的守护goroutine
func (tw *TimeWheel) Stop() {
	tw.stopChan <- struct{}{}
}

// AddJob 线程安全的添加任务
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChan <- &task{
		delay: delay,
		key:   key,
		job:   job,
	}
}

// RemoveJob 线程安全的移除任务
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskCh <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickerHandler()
		case task := <-tw.addTaskChan:
			tw.addTask(task)
		case key := <-tw.removeTaskCh:
			tw.removeTask(key)
		case <-tw.stopChan:
			tw.ticker.Stop()
			return
		}
	}
}

// tickerHandler 异步处理槽上的定时任务链表
// TODO 增加超时处理
func (tw *TimeWheel) tickerHandler() {
	l := tw.slots[tw.curPos]
	if tw.curPos == tw.slotNum-1 {
		tw.curPos = 0
	} else {
		tw.curPos++
	}
	go tw.scanAndRunTask(l)
}

// scanAndRunTask 扫描时间轮槽位上的链表，并执行对应圈数的任务
// TODO 增加超时处理
func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			task.job()
		}()
		next := e.Next()
		l.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

// addTask 添加任务到时间轮
func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPosAndCircle(task.delay)
	task.circle = circle

	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:     pos,
		elemTask: e,
	}
	if task.key != "" {
		_, ok := tw.timer[task.key]
		if ok {
			tw.removeTask(task.key)
		}
	}
	tw.timer[task.key] = loc
}

// getPosAndCircle 获取delay时间在时间轮中的位置和圈数
func (tw *TimeWheel) getPosAndCircle(delay time.Duration) (int, int) {
	delaySec := int(delay.Seconds())
	intervalSec := int(tw.interval.Seconds())
	circle := int(delaySec / intervalSec / tw.slotNum)
	pos := (tw.curPos + int(delaySec/intervalSec)) % tw.slotNum
	return pos, circle
}

// removeTask 从时间轮中删除键值为key任务
func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	// key不存在
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	// 从链表中删除
	l.Remove(pos.elemTask)
	// 从哈希表中删除
	delete(tw.timer, key)
}
