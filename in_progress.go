package events

import (
	"sync"
	"time"

	"github.com/muir/gwrap"
)

// handlersInProgressM is used to track how long handlers are taking to complete their
// work. This is used to update Prometheus metrics. The timings are updated every 10
// seconds -- we only really care about in-progress handlers if they're taking a long time.
var handlersInProgressM gwrap.SyncMap[handlerAndTopic, *handlerInProgressQueue]

const updateFrequency = 10 * time.Second

func init() {
	go func() {
		for {
			time.Sleep(updateFrequency)
			handlersInProgressM.Range(func(ht handlerAndTopic, q *handlerInProgressQueue) bool {
				q.mu.Lock()
				defer q.mu.Unlock()
				if q.queue.Len() == 0 {
					LongestHandlerLatency.WithLabelValues(ht.handlerName, ht.topic).Set(0)
				} else {
					item := q.queue.Dequeue()
					LongestHandlerLatency.WithLabelValues(ht.handlerName, ht.topic).Set(float64(time.Since(item.startTime) / time.Second))
					q.queue.Enqueue(item, item.startTime.UnixNano())
				}
				return true
			})
		}
	}()
}

type handlerInProgressQueue struct {
	mu    sync.Mutex
	queue *gwrap.PriorityQueue[int64, *handlerInProgressItem]
}

type handlerAndTopic struct {
	handlerName string
	topic       string
}

type handlerInProgressItem struct {
	gwrap.PQItemEmbed[int64]
	handlerTopic handlerAndTopic
	startTime    time.Time
	q            *handlerInProgressQueue
}

func noteHandlerStart(topic string, handlerName string) *handlerInProgressItem {
	now := time.Now()
	item := &handlerInProgressItem{
		handlerTopic: handlerAndTopic{
			handlerName: handlerName,
			topic:       topic,
		},
		startTime: now,
	}
	q, _ := handlersInProgressM.LoadOrStore(item.handlerTopic, &handlerInProgressQueue{
		queue: gwrap.NewPriorityQueue[int64, *handlerInProgressItem](),
	})
	item.q = q
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue.Enqueue(item, now.UnixNano())
	return item
}

func noteHandlerEnd(item *handlerInProgressItem) {
	item.q.mu.Lock()
	defer item.q.mu.Unlock()
	item.q.queue.Remove(item)
}
