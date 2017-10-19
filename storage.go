package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"golang.skroutz.gr/skroutz/downloader/job"

	"github.com/go-redis/redis"
)

const (
	// Each aggregation has a corresponding Redis Hash named in the form
	// "<AggrKeyPrefix><aggregation-id>" and containing various information
	// about the aggregation itself (eg. its limit).
	AggrKeyPrefix = "aggr:"

	// Job IDs of an individual aggregation exist in a Redis List named
	// in the form "<JobsKeyPrefix><aggregation-id>".
	JobsKeyPrefix = "jobs:"

	// Each Job has a corresponding Redis Hash named in the form
	// "<JobKeyPrefix><job-id>"
	JobKeyPrefix = "job:"

	// IDs of jobs that are completed and their callback is to be executed
	// are in this Redis List.
	//
	// TODO: this introduces coupling with the notifier. See how we can
	// separate it
	CallbackQueue = "CallbackQueue"
)

type QueueEmptyError string

func (err QueueEmptyError) Error() string {
	return fmt.Sprintf("Queue %s is empty", err)
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
func (s *Storage) SaveJob(j *job.Job) error {
	m, err := jobToMap(j)
	if err != nil {
		return err
	}
	scmd := s.Redis.HMSet(JobKeyPrefix+j.ID, m)
	_, err = scmd.Result()
	return err
}

func jobFromMap(m map[string]string) (job.Job, error) {
	var err error
	j := job.Job{}
	for k, v := range m {
		switch k {
		case "ID":
			j.ID = v
		case "URL":
			j.URL = v
		case "AggrID":
			j.AggrID = v
		case "DownloadState":
			j.DownloadState = job.State(v)
		case "DownloadCount":
			j.DownloadCount, err = strconv.Atoi(v)
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
			j.CallbackState = job.State(v)
		case "Extra":
			j.Extra = v
		default:
			return j, fmt.Errorf("Field %s with value %s was not found in Job struct", k, v)
		}
	}
	return j, nil
}

// GetJob fetches the Job with the given id from Redis.
func (s *Storage) GetJob(id string) (job.Job, error) {
	cmd := s.Redis.HGetAll(JobKeyPrefix + id)
	return jobFromMap(cmd.Val())
}

// Exists checks if a job exists in Redis
func (s *Storage) JobExists(j *job.Job) (bool, error) {
	res, err := s.Redis.Exists(JobKeyPrefix + j.ID).Result()

	if err != nil {
		return false, err
	}

	return res > 0, nil
}

// QueuePendingDownload sets the state of a job to "Pending", saves it and
// adds it to its aggregation queue
func (s *Storage) QueuePendingDownload(j *job.Job) error {
	j.DownloadState = job.StatePending
	err := s.SaveJob(j)
	if err != nil {
		return err
	}
	intcmd := s.Redis.RPush(JobsKeyPrefix+j.AggrID, j.ID)
	return intcmd.Err()
}

// QueuePendingCallback sets the state of a job to "Pending", saves it and adds it to its aggregation queue
func (s *Storage) QueuePendingCallback(j *job.Job) error {
	j.CallbackState = job.StatePending
	err := s.SaveJob(j)
	if err != nil {
		return err
	}
	return s.Redis.RPush(CallbackQueue, j.ID).Err()
}

// SetState changes the current Job state to the provided value and reports any errors
func (s *Storage) SetState(j *job.Job, state job.State, meta ...string) error {
	j.DownloadState = state
	j.Meta = strings.Join(meta, "\n")
	return s.SaveJob(j)
}

// SetCallbackState changes the current Job state to the provided value and reports any errors
func (s *Storage) SetCallbackState(j *job.Job, state job.State, meta ...string) error {
	j.CallbackState = state
	j.Meta = strings.Join(meta, "\n")
	return s.SaveJob(j)
}

// PopCallback attempts to pop a Job from the callback queue.
// If it succeeds the job with the popped ID is returned.
//
// TODO: this should be a method on notifier
func (s *Storage) PopCallback() (job.Job, error) {
	cmd := s.Redis.LPop(CallbackQueue)
	if err := cmd.Err(); err != nil {
		if cmd.Err().Error() != "redis: nil" {
			return job.Job{}, fmt.Errorf("Could not pop from redis queue: %s", cmd.Err().Error())
		}
		return job.Job{}, QueueEmptyError(CallbackQueue)
	}

	return s.GetJob(cmd.Val())
}

// GetAggregation fetches from Redis the aggregation denoted by id. If the
// aggregation was not found, an error is returned.
func (s *Storage) GetAggregation(id string) (job.Aggregation, error) {
	// TODO(agis): this shouldn't be needed. We can get rid of it if we
	// move the `RedisKey` method out of `Aggregation`
	aggr := job.Aggregation{ID: id}

	cmd := s.Redis.HGet(AggrKeyPrefix+id, "Limit")
	err := cmd.Err()
	if err != nil {
		return job.Aggregation{}, err
	}

	limit, err := strconv.Atoi(cmd.Val())
	if err != nil {
		return job.Aggregation{}, err
	}
	aggr.Limit = limit

	return aggr, nil
}

// Save updates/creates the current aggregation in redis.
func (s *Storage) SaveAggregation(a *job.Aggregation) error {
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
func (s *Storage) AggregationExists(a *job.Aggregation) (bool, error) {
	res, err := s.Redis.Exists(AggrKeyPrefix + a.ID).Result()

	if err != nil {
		return false, err
	}

	return res > 0, nil
}

// PopJob attempts to pop a Job for that aggregation.
// If it succeeds the job with the popped ID is returned.
func (s *Storage) PopJob(a *job.Aggregation) (job.Job, error) {
	cmd := s.Redis.LPop(JobsKeyPrefix + a.ID)
	if err := cmd.Err(); err != nil {
		if cmd.Err().Error() != "redis: nil" {
			return job.Job{}, fmt.Errorf("Could not pop from redis queue: %s", cmd.Err().Error())
		}
		return job.Job{}, QueueEmptyError(redisJobsKey(a))
	}
	return s.GetJob(cmd.Val())
}

// Return the Redis jobs list key
func redisJobsKey(a *job.Aggregation) string {
	return JobsKeyPrefix + a.ID
}

func jobToMap(j *job.Job) (map[string]interface{}, error) {
	out := make(map[string]interface{})

	v := reflect.ValueOf(j)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("jobToMap only accepts structs; got %T", v)
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
