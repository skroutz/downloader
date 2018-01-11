// errors contains types and interfaces representing download errors.
// It is used in the processor to encapsulate information on errors being retriable and/or
// internal.
package errors

import "fmt"

// DownloadError is the interface that encapsulate the bahaviour that must be met by any download error.
type DownloadError interface {
	IsRetriable() bool
	IsInternal() bool
	Err() error
	Error() string
}

// downloadError implements the DownloadError interface.
// It encapsulates an error and gives it more context my describing
// the phase in which it occured.
type downloadError struct {
	err       error
	phase     string
	retriable bool
	internal  bool
}

// Error returns a string created from the downloadError's attributes.
func (e downloadError) Error() string {
	return fmt.Sprintf("Error while %s, %s, retriable: %t, internal: %t", e.phase, e.err, e.retriable, e.internal)
}

// IsRetriable exposes the current downloadError's retriable attribute.
func (e downloadError) IsRetriable() bool {
	return e.retriable
}

// IsInternal exposes the current downloadError's internal attribute.
func (e downloadError) IsInternal() bool {
	return e.internal
}

// Retriable returns a retriable copy of the current downloadError.
func (e downloadError) Retriable() downloadError {
	e.retriable = true
	return e
}

// Internal returns a internal copy of the current downloadError.
func (e downloadError) Internal() downloadError {
	e.internal = true
	return e
}

// Err returns the raw error wrapped by the current downloadError.
func (e downloadError) Err() error {
	return e.err
}

// E creates and returns a new downloadError with the given phase and err.
// The created downloadError is neither retriable nor internal.
func E(phase string, err error) downloadError {
	return downloadError{phase: phase, err: err}
}

// Errorf is a convenience function that creates a new downloadError with given phase
// automatically formatting the given arguments to the downloadErrors err field.
func Errorf(phase string, pattern string, args ...interface{}) downloadError {
	return E(phase, fmt.Errorf(pattern, args...))
}
