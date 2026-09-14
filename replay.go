package events

import (
	"context"
	stderrors "errors"
	"sort"
	"time"

	"github.com/memsql/errors"
	"github.com/segmentio/kafka-go"

	"github.com/singlestore-labs/events/eventmodels"
)

// ReplayCursor stores the next offset to read for each topic partition.
type ReplayCursor map[int]int64

// ReplayRequest identifies the key and offsets from which replay should resume.
type ReplayRequest struct {
	Key    string
	Cursor ReplayCursor
}

type replayHandlerInfo struct {
	topic string
}

func (i replayHandlerInfo) Name() string          { return "replay" }
func (i replayHandlerInfo) BaseTopic() string     { return i.topic }
func (i replayHandlerInfo) ConsumerGroup() string { return "" }
func (i replayHandlerInfo) IsDeadLetter() bool    { return false }

// CurrentReplayCursor returns a snapshot of the next offset for every partition
// in topic. Events produced after this snapshot can be read by Replay.
func (lib *Library[ID, TX, DB]) CurrentReplayCursor(ctx context.Context, topic string) (ReplayCursor, error) {
	if err := lib.start(ctx, "get replay cursor for topic (%s)", topic); err != nil {
		return nil, err
	}
	if err := lib.ValidateTopics(ctx, []string{topic}); err != nil {
		return nil, err
	}

	partitions, err := lib.dialer().LookupPartitions(ctx, "tcp", lib.brokers[0], lib.addPrefix(topic))
	if err != nil {
		return nil, errors.Errorf("event library failed to list partitions for replay topic (%s): %w", topic, err)
	}

	cursor := make(ReplayCursor, len(partitions))
	for _, partition := range partitions {
		conn, err := lib.dialer().DialLeader(ctx, "tcp", lib.brokers[0], lib.addPrefix(topic), partition.ID)
		if err != nil {
			return nil, errors.Errorf("event library failed to connect to replay topic (%s) partition (%d): %w", topic, partition.ID, err)
		}
		offset, readErr := conn.ReadLastOffset()
		closeErr := conn.Close()
		if readErr != nil {
			return nil, errors.Errorf("event library failed to read end offset for replay topic (%s) partition (%d): %w", topic, partition.ID, readErr)
		}
		if closeErr != nil {
			return nil, errors.Errorf("event library failed to close replay topic (%s) partition (%d): %w", topic, partition.ID, closeErr)
		}
		cursor[partition.ID] = offset
	}
	return cursor, nil
}

// Replay delivers a stable topic snapshot through handler. The bound topic
// handler performs the same type-safe decoding used by normal event consumers.
// The returned cursor points immediately after the snapshot.
func (lib *Library[ID, TX, DB]) Replay(
	ctx context.Context,
	request ReplayRequest,
	handler eventmodels.HandlerInterface,
) (cursor ReplayCursor, err error) {
	topic := handler.GetTopic()
	startedAt := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
			if ctx.Err() != nil && stderrors.Is(err, ctx.Err()) {
				result = "cancelled"
			}
		}
		ReplayCounts.WithLabelValues(topic, result).Inc()
		ReplayDuration.WithLabelValues(topic, result).Observe(time.Since(startedAt).Seconds())
	}()

	if err := lib.start(ctx, "replay topic (%s)", topic); err != nil {
		return nil, err
	}
	if err := lib.ValidateTopics(ctx, []string{topic}); err != nil {
		return nil, err
	}
	if handler.Batch() {
		return nil, errors.Errorf("event library replay does not support batch handlers for topic (%s)", topic)
	}

	prefixedTopic := lib.addPrefix(topic)
	partitions, err := lib.dialer().LookupPartitions(ctx, "tcp", lib.brokers[0], prefixedTopic)
	if err != nil {
		return nil, errors.Errorf("event library failed to list partitions for replay topic (%s): %w", topic, err)
	}

	cursor = make(ReplayCursor, len(partitions))
	messages := make([]*kafka.Message, 0)
	for _, partition := range partitions {
		partitionMessages, endOffset, err := lib.replayPartition(ctx, prefixedTopic, request, partition.ID)
		if err != nil {
			return nil, err
		}
		cursor[partition.ID] = endOffset
		messages = append(messages, partitionMessages...)
	}

	// Kafka offsets order events within a partition. Timestamps only provide an
	// approximate ordering between partitions; cursor correctness uses offsets.
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].Partition == messages[j].Partition {
			return messages[i].Offset < messages[j].Offset
		}
		if messages[i].Time.Equal(messages[j].Time) {
			return messages[i].Partition < messages[j].Partition
		}
		return messages[i].Time.Before(messages[j].Time)
	})

	handler.SetLibrary(libraryInterface[ID, TX, DB]{lib})
	handlerInfo := replayHandlerInfo{topic: topic}
	for _, message := range messages {
		handlerErrors := handler.Handle(ctx, handlerInfo, []*kafka.Message{message})
		if handlerErrors[0] != nil {
			return nil, handlerErrors[0]
		}
	}
	return cursor, nil
}

func (lib *Library[ID, TX, DB]) replayPartition(
	ctx context.Context,
	topic string,
	request ReplayRequest,
	partition int,
) (messages []*kafka.Message, endOffset int64, err error) {
	conn, err := lib.dialer().DialLeader(ctx, "tcp", lib.brokers[0], topic, partition)
	if err != nil {
		return nil, 0, errors.Errorf("event library failed to connect to replay topic (%s) partition (%d): %w", topic, partition, err)
	}
	defer func() {
		closeErr := conn.Close()
		if err == nil && closeErr != nil {
			err = errors.Errorf("event library failed to close replay topic (%s) partition (%d): %w", topic, partition, closeErr)
		}
	}()

	firstOffset, err := conn.ReadFirstOffset()
	if err != nil {
		return nil, 0, errors.Errorf("event library failed to read first offset for replay topic (%s) partition (%d): %w", topic, partition, err)
	}
	endOffset, err = conn.ReadLastOffset()
	if err != nil {
		return nil, 0, errors.Errorf("event library failed to read end offset for replay topic (%s) partition (%d): %w", topic, partition, err)
	}
	startOffset, ok := request.Cursor[partition]
	if !ok || startOffset < firstOffset {
		startOffset = firstOffset
	}
	if startOffset > endOffset {
		return nil, 0, errors.Errorf("event library replay cursor offset (%d) is after topic (%s) partition (%d) end offset (%d)", startOffset, topic, partition, endOffset)
	}
	if startOffset == endOffset {
		return nil, endOffset, nil
	}
	if _, err := conn.Seek(startOffset, kafka.SeekAbsolute); err != nil {
		return nil, 0, errors.Errorf("event library failed to seek replay topic (%s) partition (%d) to offset (%d): %w", topic, partition, startOffset, err)
	}

	for startOffset < endOffset {
		readDeadline := time.Now().Add(transactionalReadTimeout)
		if deadline, ok := ctx.Deadline(); ok && deadline.Before(readDeadline) {
			readDeadline = deadline
		}
		if err := conn.SetReadDeadline(readDeadline); err != nil {
			return nil, 0, errors.Errorf("event library failed to set replay deadline for topic (%s) partition (%d): %w", topic, partition, err)
		}
		message, err := conn.ReadMessage(maxBytes)
		if err != nil {
			return nil, 0, errors.Errorf("event library failed to replay topic (%s) partition (%d): %w", topic, partition, err)
		}
		ReplayMessagesScannedCounts.WithLabelValues(lib.removePrefix(topic)).Inc()
		startOffset = message.Offset + 1
		if request.Key != "" && string(message.Key) != request.Key {
			continue
		}
		messages = append(messages, &message)
	}
	return messages, endOffset, nil
}
