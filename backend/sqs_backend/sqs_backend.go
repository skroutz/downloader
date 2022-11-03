package sqsbackend

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/skroutz/downloader/job"
)

// Backend notifies about a job completion by sending payload to sqs.
type Backend struct {
	svc     *sqs.SQS
	reports chan job.Callback
}

// ID returns "sqs".
func (b *Backend) ID() string {
	return "sqs"
}

// Start starts the backend by creating a producer,
// given a set of options provided by the configuration.
func (b *Backend) Start(ctx context.Context, cfg map[string]interface{}) error {
	region, ok := cfg["region"].(string)
	if !ok {
		return errors.New("region must be a string")
	}

	// Create a session that gets credential values from ~/.aws/credentials
	// and the default region from ~/.aws/config
	sqsSession, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return err
	}

	b.reports = make(chan job.Callback)
	b.svc = sqs.New(sqsSession)

	return nil
}

// Notify produces an SQS message
func (b *Backend) Notify(url string, cbInfo job.Callback) error {
	payload, err := cbInfo.Bytes()
	if err != nil {
		cbInfo.Delivered = false
		cbInfo.DeliveryError = err.Error()
		return err
	}

	// Returns
	//   MD5OfMessageBody: "b4c983632299c352f8e36cc7463779a1",
	//   MessageId: "35316d5a-ab64-41ca-85f9-247136e09e56"
	// TODO: not sure if we want to use these. Ignore for now
	_, err = b.svc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(payload[:])),
		QueueUrl:    aws.String(url),
	})

	if err != nil {
		err = fmt.Errorf("Got an error sending the message: %s", err.Error())
		cbInfo.Delivered = false
		cbInfo.DeliveryError = err.Error()
		return err
	}

	cbInfo.Delivered = true
	cbInfo.DeliveryError = ""
	b.reports <- cbInfo
	return nil
}

// DeliveryReports returns a channel of emmited callback events
func (b *Backend) DeliveryReports() <-chan job.Callback {
	return b.reports
}

// Stop shuts down the backend
func (b *Backend) Stop() error {
	close(b.reports)
	return nil
}
