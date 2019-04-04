package backend

import (
	"context"

	"github.com/skroutz/downloader/job"
)

// Backend is the interface that wraps the basic Notify method.
//
// Backend implementations are responsible for notifying about a job
// completion through some notification channel (eg. HTTP, Kafka).
type Backend interface {
	// Start() initializes the backend. Start() must be called once, before
	// any calls to Notify.
	Start(context.Context, map[string]interface{}) error

	// Notify() notifies about a job completion. Depending on the underlying
	// implementation, Notify might be an asynchronous operation so a nil
	// error does NOT necessarily mean the notification was delivered.
	// To check for the result of a notification use DeliveryReports().
	//
	// Implementations should peek job.CallbackDst to determine the
	// destination of the notification.
	Notify(string, job.Callback) error

	// ID returns a constant string used as an identifier for the
	// concrete backend implementation.
	ID() string

	// DeliveryReports() is used to communicate the results of notifications.
	//
	// Even if a message received from this channel is successful that
	// does not mean that the callback has been consumed on the other end.
	DeliveryReports() <-chan job.Callback

	// Stop() closes the delivery reports channel and performs finalization
	// actions. After calling Stop() the backend is no longer usable.
	Stop() error
}
