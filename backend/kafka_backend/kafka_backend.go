package kafkabackend

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/skroutz/downloader/job"
)

// FlushTimeout is the timeout we give to our kafka producer
// to flush pending messages.
const FlushTimeout = 5000

// Backend notifies about a job completion by producing to a Kafka topic.
type Backend struct {
	producer *kafka.Producer
	reports  chan job.Callback
	eventsWg *sync.WaitGroup
}

// ID returns "kafka".
func (b *Backend) ID() string {
	return "kafka"
}

// Start starts the backend by creating a producer,
// given a set of options provided by the configuration.
func (b *Backend) Start(ctx context.Context, cfg map[string]interface{}) error {
	var err error

	kafkaCfg := make(kafka.ConfigMap)
	for k, v := range cfg {
		err := kafkaCfg.SetKey(k, v)
		if err != nil {
			return err
		}
	}

	b.producer, err = kafka.NewProducer(&kafkaCfg)
	if err != nil {
		return err
	}

	b.reports = make(chan job.Callback)
	b.eventsWg = new(sync.WaitGroup)

	// start a go routine to monitor Kafka's Events channel
	b.eventsWg.Add(1)
	go func() {
		defer b.eventsWg.Done()
		b.transformStream(ctx)
	}()

	return nil
}

// Notify produces a Kafka message to topic.
func (b *Backend) Notify(topic string, cbInfo job.Callback) error {
	payload, err := cbInfo.Bytes()
	if err != nil {
		return err
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}

	return b.producer.Produce(message, nil)
}

// DeliveryReports returns a channel of emmited callback events
func (b *Backend) DeliveryReports() <-chan job.Callback {
	return b.reports
}

// Stop gracefully terminates b after flushing any outstanding messages to Kafka.
// An error is returned if (and only if) not all messages were flushed.
func (b *Backend) Stop() error {
	var err error

	unflushed := b.producer.Flush(FlushTimeout)
	if unflushed > 0 {
		err = fmt.Errorf("After %d ms there were still %d unflushed messages", FlushTimeout, unflushed)
	}

	b.producer.Close()
	b.eventsWg.Wait()
	close(b.reports)

	return err
}

// transformStream iterates over the Events channel of Kafka, transforms
// each message to a callback info object and enqueues the callback object
// to b.reports channel.
func (b *Backend) transformStream(ctx context.Context) {
	for {
		select {
		case e, ok := <-b.producer.Events():
			if !ok {
				return
			}

			switch ev := e.(type) {
			case *kafka.Message:
				var cbInfo job.Callback

				err := json.Unmarshal(ev.Value, &cbInfo)
				if err != nil {
					cbInfo.Delivered = false
					cbInfo.DeliveryError = fmt.Sprintf("Could not unmarshall Value %s to callback object", ev.Value)
				} else {
					cbInfo.Delivered = true
					cbInfo.DeliveryError = ""

					if ev.TopicPartition.Error != nil {
						cbInfo.Delivered = false
						cbInfo.DeliveryError = ev.TopicPartition.Error.Error()
					}
				}

				b.reports <- cbInfo
			}
		}
	}
}
