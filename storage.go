package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

type QueueEmptyError string

func (err QueueEmptyError) Error() string {
	return fmt.Sprintf("Queue %s is empty", err)
}

const (
	// Each aggregation has a corresponding Redis Hash named in the form
	// "<AggrKeyPrefix><aggregation-id>" and containing various information
	// about the aggregation itself (eg. its limit).
	AggrKeyPrefix = "aggr:"

	// Job IDs of an individual aggregation exist in a Redis List named
	// in the form "<JobsKeyPrefix><aggregation-id>".
	JobsKeyPrefix = "jobs:"

	// Each Job has a corresponding Redis Hash named in the form of
	// "<JobKeyPrefix><job-id>"
	JobKeyPrefix = "job:"

	// IDs of jobs that are completed and their callback is to be executed
	// are in this Redis List.
	//
	// TODO: this introduces coupling with the notifier. See how we can
	// separate it
	CallbackQueue = "CallbackQueue"
)

// CallbackInfo holds the info to be posted back to the provided callback url of the caller
type CallbackInfo struct {
	Success     bool   `json:"success"`
	Error       string `json:"error"`
	Extra       string `json:"extra"`
	DownloadURL string `json:"download_url"`
}

type Storage struct {
	Redis *redis.Client
}

// NewStorage returns a new Storage that can communicate with Redis. If Redis
// is not up an error will be returned.
//
// Callers should set right after set AggrKeyPrefix, JobKeyPrefix and
// CallbackQueue fields on the returned storage.
func NewStorage(r *redis.Client) (*Storage, error) {
	if ping := r.Ping(); ping.Err() != nil || ping.Val() != "PONG" {
		if ping.Err() != nil {
			return nil, fmt.Errorf("Could not ping Redis Server successfully: %v", ping.Err())
		}
		return nil, fmt.Errorf("Could not ping Redis Server successfully: Expected PONG, received %s", ping.Val())
	}

	return &Storage{Redis: r}, nil
}

// SaveJob updates or creates j in Redis.
func (s *Storage) SaveJob(j *Job) error {
	m, err := j.toMap()
	if err != nil {
		return err
	}
	scmd := s.Redis.HMSet(JobKeyPrefix+j.ID, m)
	_, err = scmd.Result()
	return err
}

func (j *Job) toMap() (map[string]interface{}, error) {
	out := make(map[string]interface{})

	v := reflect.ValueOf(j)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ToMap only accepts structs; got %T", v)
	}

	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		// gets us a StructField
		fi := typ.Field(i)
		// set key of map to value in struct field
		out[fi.Name] = v.Field(i).Interface()
	}
	return out, nil
}

func jobFromMap(m map[string]string) (Job, error) {
	var err error
	j := Job{}
	for k, v := range m {
		switch k {
		case "ID":
			j.ID = v
		case "URL":
			j.URL = v
		case "AggrID":
			j.AggrID = v
		case "DownloadState":
			j.DownloadState = State(v)
		case "RetryCount":
			j.RetryCount, err = strconv.Atoi(v)
			if err != nil {
				return j, fmt.Errorf("Could not decode struct from map: %v", err)
			}
		case "Meta":
			j.Meta = v
		case "CallbackURL":
			j.CallbackURL = v
		case "CallbackCount":
			j.CallbackCount, err = strconv.Atoi(v)
			if err != nil {
				return j, fmt.Errorf("Could not decode struct from map: %v", err)
			}
		case "CallbackState":
			j.CallbackState = State(v)
		case "Extra":
			j.Extra = v
		default:
			return j, fmt.Errorf("Field %s with value %s was not found in Job struct", k, v)
		}
	}
	return j, nil
}

// GetJob fetches the Job with the given id from Redis.
func (s *Storage) GetJob(id string) (Job, error) {
	cmd := s.Redis.HGetAll(JobKeyPrefix + id)
	return jobFromMap(cmd.Val())
}

// Exists checks if a job exists in Redis
func (s *Storage) JobExists(j *Job) (bool, error) {
	res, err := s.Redis.Exists(JobKeyPrefix + j.ID).Result()

	if err != nil {
		return false, err
	}

	return res > 0, nil
}

// QueuePendingDownload sets the state of a job to "Pending", saves it and
// adds it to its aggregation queue
func (s *Storage) QueuePendingDownload(j *Job) error {
	j.DownloadState = StatePending
	err := s.SaveJob(j)
	if err != nil {
		return err
	}
	intcmd := s.Redis.RPush(JobsKeyPrefix+j.AggrID, j.ID)
	return intcmd.Err()
}

// QueuePendingCallback sets the state of a job to "Pending", saves it and adds it to its aggregation queue
func (s *Storage) QueuePendingCallback(j *Job) error {
	j.CallbackState = StatePending
	err := s.SaveJob(j)
	if err != nil {
		return err
	}
	return s.Redis.RPush(CallbackQueue, j.ID).Err()
}

// SetState changes the current Job state to the provided value and reports any errors
func (s *Storage) SetState(j *Job, state State, meta ...string) error {
	j.DownloadState = state
	j.Meta = strings.Join(meta, "\n")
	return s.SaveJob(j)
}

// SetCallbackState changes the current Job state to the provided value and reports any errors
func (s *Storage) SetCallbackState(j *Job, state State, meta ...string) error {
	j.CallbackState = state
	j.Meta = strings.Join(meta, "\n")
	return s.SaveJob(j)
}

// callbackInfo validates that the job is good for callback and
// return callbackInfo to the caller
func (j *Job) callbackInfo() (CallbackInfo, error) {
	if j.DownloadState != StateSuccess && j.DownloadState != StateFailed {
		return CallbackInfo{}, fmt.Errorf("Invalid Job State %s", j.DownloadState)
	}

	return CallbackInfo{
		Success:     j.DownloadState == StateSuccess,
		Error:       j.Meta,
		Extra:       j.Extra,
		DownloadURL: j.downloadURL(),
	}, nil
}

// downloadURL constructs the actual download URL to be provided to the user.
// TODO: Actually make it smart
func (j *Job) downloadURL() string {
	return fmt.Sprintf("http://localhost/%s", j.ID)
}

// PopCallback attempts to pop a Job from the callback queue.
// If it succeeds the job with the popped ID is returned.
//
// TODO: this should be a method on notifier
func (s *Storage) PopCallback() (Job, error) {
	cmd := s.Redis.LPop(CallbackQueue)
	if err := cmd.Err(); err != nil {
		if cmd.Err().Error() != "redis: nil" {
			return Job{}, fmt.Errorf("Could not pop from redis queue: %s", cmd.Err().Error())
		}
		return Job{}, QueueEmptyError(CallbackQueue)
	}

	return s.GetJob(cmd.Val())
}

// GetAggregation fetches from Redis the aggregation denoted by id. If the
// aggregation was not found, an error is returned.
func (s *Storage) GetAggregation(id string) (Aggregation, error) {
	// TODO(agis): this shouldn't be needed. We can get rid of it if we
	// move the `RedisKey` method out of `Aggregation`
	aggr := Aggregation{ID: id}

	cmd := s.Redis.HGet(AggrKeyPrefix+id, "Limit")
	err := cmd.Err()
	if err != nil {
		return Aggregation{}, err
	}

	limit, err := strconv.Atoi(cmd.Val())
	if err != nil {
		return Aggregation{}, err
	}
	aggr.Limit = limit

	return aggr, nil
}

// Return the Redis key
// TODO: ELIMINATE USAGES OF THIS
func (aggr *Aggregation) RedisKey() string {
	return AggrKeyPrefix + aggr.ID
}

// Return the Redis job list key
// TODO: ELIMINATE THIS
func (aggr *Aggregation) RedisJobsKey() string {
	return JobsKeyPrefix + aggr.ID
}

// Save updates/creates the current aggregation in redis.
func (s *Storage) SaveAggregation(a *Aggregation) error {
	cmd := s.Redis.HSet(AggrKeyPrefix+a.ID, "Limit", a.Limit)
	return cmd.Err()
}

// Remove deletes the aggregation key from Redis
// It does not remove the jobs list for the aggregation
// since we never want to lose track of already queued jobs
//
// TODO: Unused?
func (s *Storage) RemoveAggregation(id string) error {
	return s.Redis.Del(AggrKeyPrefix + id).Err()
}

// Exists checks if the given aggregation exists in the Redis
func (s *Storage) AggregationExists(a *Aggregation) (bool, error) {
	res, err := s.Redis.Exists(AggrKeyPrefix + a.ID).Result()

	if err != nil {
		return false, err
	}

	return res > 0, nil
}

// PopJob attempts to pop a Job for that aggregation.
// If it succeeds the job with the popped ID is returned.
func (s *Storage) PopJob(a *Aggregation) (Job, error) {
	cmd := s.Redis.LPop(JobsKeyPrefix + a.ID)
	if err := cmd.Err(); err != nil {
		if cmd.Err().Error() != "redis: nil" {
			return Job{}, fmt.Errorf("Could not pop from redis queue: %s", cmd.Err().Error())
		}
		return Job{}, QueueEmptyError(a.RedisJobsKey())
	}

	return s.GetJob(cmd.Val())
}
