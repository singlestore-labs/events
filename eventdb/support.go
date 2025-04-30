package eventdb

import "github.com/singlestore-labs/events/eventmodels"

func PreAllocateIDMap[T any](events ...eventmodels.ProducingEvent) map[string][]T {
	ids := make(map[string][]T)
	counts := make(map[string]int)
	for _, event := range events {
		counts[event.GetTopic()]++
	}
	for topic, count := range counts {
		ids[topic] = make([]T, 0, count)
	}
	return ids
}
