// Package storage is an abstraction/utility layer over Redis.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/skroutz/downloader/job"

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

	// CallbackQueue contains IDs of jobs that are completed
	// and their callback is to be executed
	// TODO: this introduces coupling with the notifier. See how we can
	// separate it
	CallbackQueue = "CallbackQueue"

	// RIPQueue contains ids of jobs to be deleted
	RIPQueue = "JobDeletionQueue"

	// Prefix for stats related entries
	statsPrefix = "stats"

	// The default aggregation limit
	aggrDefaultLimit = 4
)

var (
	// Atomically pop jobs from a sorted set (ZSET)
	//
	// Each job has a score that points to the time
	// it should be executed.
	//
	// We only pop jobs that are "ready" to execute,
	// so we can implement backoffs by scheduling jobs
	// in the future.
	//
	// Note that we return two different kind of errors,
	// EMPTY & RETRYLATER. We need this distinction in
	// order to decide if we should close the worker pool
	// or just wait a bit for new jobs.
	//
	// Both operations are 0(1) since we operate on the
	// left side of an ordered list.
	zpop = redis.NewScript(`
		local key = KEYS[1]
		local max_score = ARGV[1]

		-- Get the Job with the smallest score
		local top = redis.call("zrange", key, 0, 0, 'withscores')

		-- Empty ZSET
		if #top == 0 then
			return redis.error_reply("EMPTY")
		end

		local job = top[1]
		local score = top[2]

		-- Job is not ready yet
		if score > max_score then
			return redis.error_reply("RETRYLATER")
		end

		-- We have a Job!
		redis.call("zremrangebyrank", key, 0, 0)
		return job
		`)

	// Atomically delete the aggregation key
	//
	// Every aggregation has a corresponding job. Before deleting an
	// aggregation we want to ensure that there are no related jobs in the
	// jobs queue.
	//
	// The operation has to be executed atomically since a new job may be
	// added right before we delete the aggregation, leaving the newly added
	// job with no aggregation.
	delaggr = redis.NewScript(`
			local jobsKey = KEYS[1]
			local aggrKey = KEYS[2]

			-- Get number of jobs in the queue
			local count = redis.call("zcount", jobsKey, "-inf", "+inf")

			-- Job queue is not empty
			if count > 0 then
			  return 0
			end

			-- Remove aggregation
			redis.call("del", aggrKey)
			return 1
		`)

	// Atomically get an aggregation
	//
	// This should only return an aggregation if it is available for processing.
	// This is determined by the ExpiresAt key which contains a Unix Timestamp.
	// ExpiresAt:
	//   - false                        : Aggregation not found
	//   - ""						    : First time seeing this aggregation
	// 								      set ExpiresAt based on timeout and return it
	//   - ExpiresAt < now    : Aggregation has expired and thus is available for processing
	//                                    set ExpiresAt based on timeout and return it
	//   - ExpiresAt >= now   : Aggregation is already being processed. Return error "BEINGPROCESSED"
	aggrpop = redis.NewScript(`
			local aggr = KEYS[1]
			local timeout = KEYS[2]
			
			-- required replication method for redis < 5.0
			redis.replicate_commands()

			-- get current unix timestamp
			local cUnixTimestamp = tonumber(redis.call("time")[1])

			-- get aggregations timestamp
			local expiresAt = redis.call("hget", aggr, "ExpiresAt")

			if expiresAt == false then
				return redis.error_reply("NOTFOUND")
			elseif expiresAt == "" or tonumber(expiresAt) < cUnixTimestamp then
					redis.call("hset", aggr, "ExpiresAt", cUnixTimestamp+timeout)
					return redis.call("hgetall", aggr)
			else
				return redis.error_reply("BEINGPROCESSED")
			end
`)

	// ErrEmptyQueue is returned by ZPOP when there is no job in the queue
	ErrEmptyQueue = errors.New("Queue is empty")
	// ErrRetryLater is returned by ZPOP when there are only future jobs in the queue
	ErrRetryLater = errors.New("Retry again later")
	// ErrNotFound is returned by GetJob and GetAggregation when a requested
	// job, or aggregation respectively is not found in Redis.
	ErrNotFound = errors.New("Not Found")
	// ErrAggrBeingProcessed is returned by GetAggregation when the requested aggregation
	// is being processed by another downloader processor.
	ErrAggrBeingProcessed = errors.New("Aggregation is already being processed")
)

// Storage wraps a redis.Client instance.
type Storage struct {
	// Redis Client - used both in Sentinel and non-Sentinel environments
	Redis *redis.Client
	// Sentinel Client used to subscribe to failover events
	SentinelClient *redis.Client
}

// New returns a new Storage that can communicate with Redis. If Redis
// is not up an error will be returned.
//
// Callers should set right after set AggrKeyPrefix, JobKeyPrefix and
// CallbackQueue fields on the returned storage.
func New(r *redis.Client) (*Storage, error) {
	if ping := r.Ping(); ping.Err() != nil || ping.Val() != "PONG" {
		if ping.Err() != nil {
			return nil, fmt.Errorf("Could not ping Redis Server successfully: %v", ping.Err())
		}
		return nil, fmt.Errorf("Could not ping Redis Server successfully: Expected PONG, received %s", ping.Val())
	}

	return &Storage{Redis: r}, nil
}

// Creates a new Storage instance to communicate with Redis. Does the same things as
// storage.New but also creates a sentinel redis client to subscribe to failover events.
func NewWithSentinel(sentinelAddrs []string, masterName string, clientName string) (*Storage, error) {
	setClientName := func(c *redis.Conn) error {
		ok, err := c.ClientSetName(clientName).Result()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("Error setting Redis client name to " + clientName)
		}
		return nil
	}

	redis_c := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
		OnConnect:     setClientName,
	})

	if err := redis_c.Ping().Err(); err != nil {
		return nil, err
	}

	sent_c, err := sentinelClient(sentinelAddrs)
	if err != nil {
		return nil, err
	}

	return &Storage{Redis: redis_c, SentinelClient: sent_c}, nil

}

// Create a new Redis Client and update the existing Storage instance. This will update
// redis client on all goroutines as well.
func (s *Storage) ReEstablishRedisConnection(sentinelAddrs []string, masterName string, clientName string) error {
	s_n, err := NewWithSentinel(sentinelAddrs, masterName, clientName)
	if err != nil {
		return err
	}

	s.Redis = s_n.Redis

	return nil
}

// Check for Sentinel events using pubsub. Notifies main routine using ch.
// This is used to detect a failover event.
func CheckSentinelFailover(pubsub *redis.PubSub, sentMaster string, ch chan<- bool) {
	for {
		message, err := pubsub.ReceiveTimeout(time.Second * 1.0)

		if err != nil {
			continue
		}

		switch msg := message.(type) {
		case *redis.Message:
			master := strings.Split(msg.Payload, " ")[0]
			if master == sentMaster {
				ch <- true
			}
		}
	}
}

// Finds the first available Sentinel and connects to it.
func sentinelClient(sentinelAddrs []string) (*redis.Client, error) {
	for _, addr := range sentinelAddrs {
		sent_c := redis.NewClient(&redis.Options{
			Addr: addr,
		})

		if err := sent_c.Ping().Err(); err != nil {
			continue
		}
		return sent_c, nil
	}

	sentAddrs := strings.Join(sentinelAddrs, ", ")
	return nil, errors.New("Error connecting to Sentinels " + sentAddrs)
}

// SaveJob updates or creates j in Redis.
//
// TODO: should we check that the corresponding aggregation exists in redis?
func (s *Storage) SaveJob(j *job.Job) error {
	m, err := structToMap(j)
	if err != nil {
		return err
	}
	return s.Redis.HMSet(JobKeyPrefix+j.ID, m).Err()
}

// GetJob fetches the job with the given id from Redis.
// In the case of ErrNotFound, the returned job has valid ID and can be used
// further.
func (s *Storage) GetJob(id string) (job.Job, error) {
	val, err := s.Redis.HGetAll(JobKeyPrefix + id).Result()
	if err != nil {
		return job.Job{}, err
	}

	if v, ok := val["ID"]; !ok || v == "" {
		return job.Job{ID: id}, ErrNotFound
	}

	return jobFromMap(val)
}

// RemoveJob removes the job key from Redis.
func (s *Storage) RemoveJob(id string) error {
	return s.Redis.Del(JobKeyPrefix + id).Err()
}

// JobExists checks if the given job exists in Redis.
// If a non-nil error is returned, the first returned value should be ignored.
func (s *Storage) JobExists(j *job.Job) (bool, error) {
	return s.exists(JobKeyPrefix + j.ID)
}

// AggregationExists checks if the given aggregation exists in Redis.
// If a non-nil error is returned, the first returned value should be ignored.
func (s *Storage) AggregationExists(a *job.Aggregation) (bool, error) {
	return s.exists(AggrKeyPrefix + a.ID)
}

// QueuePendingDownload sets the state of a job to "Pending", saves it and
// adds it to its aggregation queue.
// If a delay >0 is given, the job is queued with a higher score & actually later in time.
//
// TODO: should we check that job already exists in redis? maybe do HSET instead?
func (s *Storage) QueuePendingDownload(j *job.Job, delay time.Duration) error {
	j.DownloadState = job.StatePending
	err := s.SaveJob(j)
	if err != nil {
		return err
	}

	z := redis.Z{
		Member: j.ID,
		Score:  float64(time.Now().Add(delay).Unix()),
	}
	return s.Redis.ZAdd(JobsKeyPrefix+j.AggrID, z).Err()
}

// QueuePendingCallback sets the state of a job to "Pending", saves it and adds it to its aggregation queue
// If a delay >0 is given, the job is queued with a higher score & actually later in time.
func (s *Storage) QueuePendingCallback(j *job.Job, delay time.Duration) error {
	if j.HasCallback() {
		j.CallbackState = job.StatePending
	}
	err := s.SaveJob(j)
	if err != nil {
		return err
	}

	if j.HasCallback() {
		z := redis.Z{
			Member: j.ID,
			Score:  float64(time.Now().Add(delay).Unix()),
		}
		return s.Redis.ZAdd(CallbackQueue, z).Err()
	}

	return nil
}

// QueueJobForDeletion pushes the provided job id to RIPQueue and returns any errors
// The job deletion can be delayed by the specified delay minutes.
func (s *Storage) QueueJobForDeletion(id string, delay time.Duration) error {
	z := redis.Z{
		Member: id,
		Score:  float64(time.Now().Add(delay).Unix()),
	}
	return s.Redis.ZAdd(RIPQueue, z).Err()
}

// PopCallback attempts to pop a Job from the callback queue.
// If it succeeds the job with the popped ID is returned.
func (s *Storage) PopCallback() (job.Job, error) {
	return s.pop(CallbackQueue)
}

// PopJob attempts to pop a Job for that aggregation.
// If it succeeds the job with the popped ID is returned.
func (s *Storage) PopJob(a *job.Aggregation) (job.Job, error) {
	return s.pop(JobsKeyPrefix + a.ID)
}

// PopRip fetches a job from the RIPQueue ( if any ) and reports any errors.
// If the queue is empty an ErrEmptyQueue error is returned.
// Notice: Due to the nature of job deletion, the returned job is not guaranteed to
// be available in Redis.
func (s *Storage) PopRip() (job.Job, error) {
	j, err := s.pop(RIPQueue)
	if err != nil && err != ErrNotFound {
		return job.Job{}, err
	}

	return j, nil
}

// GetAggregation fetches from Redis the aggregation denoted by id.
// In the case of ErrNotFound, the returned aggregation has valid ID and the
// default limit.
func (s *Storage) GetAggregation(id string) (*job.Aggregation, error) {
	val, err := aggrpop.Run(s.Redis, []string{AggrKeyPrefix + id, strconv.Itoa(job.AggregationTimeout)}).Result()
	if err != nil {
		switch err.Error() {
		case "BEINGPROCESSED":
			return nil, ErrAggrBeingProcessed
		case "NOTFOUND":
			// continue, a default aggregation will be created
		default:
			return nil, err
		}
	}

	values, _ := val.([]interface{})
	aggrValues := make(map[string]string)
	for i := 0; i < len(values); i += 2 {
		aggrValues[values[i].(string)] = values[i+1].(string)
	}

	if v, ok := aggrValues["ID"]; !ok || v == "" {
		aggr, err := job.NewAggregation(id, aggrDefaultLimit, "", "")
		if err != nil {
			return nil, err
		}
		return aggr, ErrNotFound
	}

	aggr, err := AggregationFromMap(aggrValues)
	return &aggr, err
}

// SaveAggregation updates/creates the current aggregation in redis.
func (s *Storage) SaveAggregation(a *job.Aggregation) error {
	m, err := structToMap(a)
	if err != nil {
		return err
	}
	return s.Redis.HMSet(AggrKeyPrefix+a.ID, m).Err()
}

// RemoveAggregation deletes the aggregation key from Redis
func (s *Storage) RemoveAggregation(id string) error {
	_, err := delaggr.Run(s.Redis, []string{JobsKeyPrefix + id, AggrKeyPrefix + id}).Result()
	if err != nil {
		return fmt.Errorf("Could not delaggr: %s", err)
	}
	return nil
}

func AggregationFromMap(m map[string]string) (job.Aggregation, error) {
	var err error
	aggr := job.Aggregation{}
	for k, v := range m {
		switch k {
		case "ID":
			aggr.ID = v
		case "Limit":
			aggr.Limit, err = strconv.Atoi(v)
			if err != nil {
				return aggr, fmt.Errorf("Could not decode struct from map: %v", err)
			}
		case "Proxy":
			aggr.Proxy = v
		case "ExpiresAt":
			aggr.ExpiresAt = v
		default:
			return aggr, fmt.Errorf("Field %s with value %s was not found in Aggregarion struct", k, v)
		}
	}
	return aggr, nil
}

// RetryCallback resets a job's callback state and injects it back to the
// callback queue.
// If the job is not found, an error is returned.
func (s *Storage) RetryCallback(j *job.Job) error {
	exists, err := s.JobExists(j)
	if err != nil {
		return fmt.Errorf("Could not check Job existence: %s", err)
	}
	if !exists {
		return errors.New("Job doesn't exist in Redis:" + j.ID)
	}

	j.CallbackMeta = ""
	j.CallbackCount = 0
	return s.QueuePendingCallback(j, 0)
}

func structToMap(str interface{}) (map[string]interface{}, error) {
	out := make(map[string]interface{})

	v := reflect.ValueOf(str)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// we only accept structs
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("structToMap only accepts structs; got %T", v)
	}

	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		// gets us a StructField
		fi := typ.Field(i)

		// Redis only accepts stringified payloads, so maps/objects need to
		// be stringified prior to storing them in redis.
		if fi.Type.Kind() == reflect.Map {
			mapData := v.Field(i).Interface()
			bytes, err := json.Marshal(mapData)
			if err != nil {
				return nil, err
			}
			out[fi.Name] = string(bytes)
		} else {
			// set key of map to value in struct field
			out[fi.Name] = v.Field(i).Interface()
		}
	}
	return out, nil
}

// TODO: This is too fragile. Changing the name of a Job field will break this
// method. Is there a better way?
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
		case "DownloadMeta":
			j.DownloadMeta = v
		case "CallbackURL":
			j.CallbackURL = v
		case "CallbackType":
			j.CallbackType = v
		case "CallbackDst":
			j.CallbackDst = v
		case "CallbackCount":
			j.CallbackCount, err = strconv.Atoi(v)
			if err != nil {
				return j, fmt.Errorf("Could not decode struct from map: %v", err)
			}
		case "CallbackState":
			j.CallbackState = job.State(v)
		case "CallbackMeta":
			j.CallbackMeta = v
		case "Extra":
			j.Extra = v
		case "ResponseCode":
			j.ResponseCode, err = strconv.Atoi(v)
			if err != nil {
				return j, fmt.Errorf("Could not decode struct from map: %v", err)
			}
		case "MimeType":
			j.MimeType = v
		case "MaxRetries":
			if err := j.MaxRetries.UnmarshalBinary([]byte(v)); err != nil {
				return j, fmt.Errorf("Could not decode struct from map: %v", err)
			}
		case "ExtractImageSize": // Match "1" to true
			j.ExtractImageSize = v == "1"
		case "ImageSize":
			j.ImageSize = v
		case "DownloadTimeout":
			j.DownloadTimeout, err = strconv.Atoi(v)
			if err != nil {
				return j, fmt.Errorf("Could not decode struct from map: %v", err)
			}
		case "RequestHeaders":
			headers := make(map[string]string)
			err := json.Unmarshal([]byte(v), &headers)
			if err != nil {
				return j, fmt.Errorf("Could not decode request headers: %v", err)
			}
			j.RequestHeaders = headers
		case "S3Bucket":
			j.S3Bucket = v
		case "S3Region":
			j.S3Region = v
		case "DownloadURL":
			j.DownloadURL = v
		default:
			return j, fmt.Errorf("Field %s with value %s was not found in Job struct", k, v)
		}
	}
	return j, nil
}

// Checks if key exists in Redis
func (s *Storage) exists(key string) (bool, error) {
	res, err := s.Redis.Exists(key).Result()
	return res > 0, err
}

// POPs from list and returns the corresponding job
func (s *Storage) pop(list string) (job.Job, error) {
	val, err := zpop.Run(s.Redis, []string{list}, time.Now().Unix()).Result()

	if err != nil {
		switch err.Error() {
		case "EMPTY":
			return job.Job{}, ErrEmptyQueue
		case "RETRYLATER":
			return job.Job{}, ErrRetryLater
		default:
			return job.Job{}, fmt.Errorf("Could not zpop: %s", err)
		}
	}

	// ZPOP should always return a string
	jobID, ok := val.(string)
	if !ok {
		panic(fmt.Sprintf("zpop replied with '%#v', it should be a string!", val))
	}

	return s.GetJob(jobID)
}

// GetStats fetches stats prefixed entries from Redis
func (s *Storage) GetStats(id string) ([]byte, error) {
	getCmd := s.Redis.Get(strings.Join([]string{statsPrefix, id}, ":"))

	if err := getCmd.Err(); err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	return getCmd.Bytes()
}

// SetStats saves stats in Redis
func (s *Storage) SetStats(id, stats string, expiration time.Duration) error {
	return s.Redis.Set(strings.Join([]string{statsPrefix, id}, ":"), stats, expiration).Err()
}

// RequeueInProgressJobs requeues all Jobs of an Aggregation that were previously
// started but interrupted. This is achieved by setting
// DownloadState: job.StatePending
// DownlloadCount: 0
func (s *Storage) RequeueInProgressJobs(aggrID string) error {

	resetValues := map[string]interface{}{
		"DownloadState": job.StatePending,
		"DownloadCount": "0",
	}

	jobs, err := s.Redis.ZRange(JobsKeyPrefix+aggrID, 0, -1).Result()
	if err != nil {
		if err == redis.Nil {
			// no jobs available, shouldn't be here but still, nothing to do
			return nil
		} else {
			return err
		}
	}

	for _, jobId := range jobs {
		jobState, err := s.Redis.HGet(JobKeyPrefix+jobId, "DownloadState").Result()
		if err != nil {
			return err
		}

		if jobState == job.StateInProgress {
			setError := s.Redis.HMSet(JobKeyPrefix+jobId, resetValues).Err()
			if setError != nil {
				return setError
			}
		}
	}

	// Everything went fine!
	return nil
}
