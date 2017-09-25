package main

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

const (
	aggrKeyPrefix = "aggr:"
	jobKeyPrefix  = "jobs:"
)

// State represents the download & callback states.
// For valid values see constants below.
type State string

// Valid State values
const (
	StatePending    = "Pending"
	StateFailed     = "Failed"
	StateSuccess    = "Success"
	StateInProgress = "InProgress"
)

// Job represents a download.
// It is the main entity of the downloader and holds all info and state of the download
type Job struct {
	ID            string `json:"id"`
	URL           string `json:"url"`
	AggrID        string `json:"aggr_id"`
	DownloadState State  `json:"-"`
	RetryCount    int    `json:"-"`
	Meta          string `json:"-"`
	CallbackURL   string `json:"callback_url"`
	CallbackCount int    `json:"-"`
	CallbackState State  `json:"-"`
}

// Aggregation represents a rate limit unit.
type Aggregation struct {
	ID    string
	Limit int
}

// MarshalBinary is used by redis driver to marshall custom type State
func (s State) MarshalBinary() (data []byte, err error) {
	return []byte(string(s)), nil
}

// Redis must be initialized before use
var Redis *redis.Client

// InitStorage creates the Redis client singleton to be used by the storage class
func InitStorage(host string, port int) error {
	Redis = redis.NewClient(&redis.Options{Addr: strings.Join([]string{host, strconv.Itoa(port)}, ":")})
	if ping := Redis.Ping(); ping.Err() != nil || ping.Val() != "PONG" {
		if ping.Err() != nil {
			return fmt.Errorf("Could not ping Redis Server successfully: %v", ping.Err())
		}
		return fmt.Errorf("Could not ping Redis Server successfully: Expected PONG, received %s", ping.Val())
	}
	return nil
}

// Save updates/ creates the current job in redis.
func (j *Job) Save() error {
	m, err := j.toMap()
	if err != nil {
		return err
	}
	scmd := Redis.HMSet(j.ID, m)
	_, err = scmd.Result()
	return err
}

//GetJob fetches the Job with the given id from Redis
func GetJob(id string) (Job, error) {
	cmd := Redis.HGetAll(id)
	return jobFromMap(cmd.Val())
}

// Exists checks if a job exists in the DB
func (j *Job) Exists() (bool, error) {
	res, err := Redis.Exists(j.ID).Result()

	if err != nil {
		return false, err
	}

	return res > 0, nil
}

// QueuePendingDownload sets the state of a job to "Pending", saves it and adds it to its aggregation queue
func (j *Job) QueuePendingDownload() error {
	j.DownloadState = StatePending
	err := j.Save()
	if err != nil {
		return err
	}
	intcmd := Redis.RPush(jobKeyPrefix+j.AggrID, j.ID)
	return intcmd.Err()
}

// PopJob attempts to pop a Job ID from the specified queue.
// If it succeeds the job with the popped ID is returned.
func PopJob(queue string) (Job, error) {
	cmd := Redis.LPop(queue)
	if err := cmd.Err(); err != nil {
		if cmd.Err().Error() != "redis: nil" {
			return Job{}, fmt.Errorf("Could not pop from redis queue: %s", cmd.Err().Error())
		}
		//No jobs popped
		return Job{}, errors.New("Queue is empty")
	}

	return GetJob(cmd.Val())
}

// SetStateWithMeta changes the current Job state to the provided value, updates the Meta field and reports any errors
func (j *Job) SetStateWithMeta(state State, meta string) error {
	j.DownloadState = state
	j.Meta = meta
	return j.Save()
}

// SetState changes the current Job state to the provided value and reports any errors
func (j *Job) SetState(state State) error {
	j.DownloadState = state
	return j.Save()
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
		default:
			return j, fmt.Errorf("Field %s with value %s was not found in Job struct", k, v)
		}
	}
	return j, nil
}

// GetAggregation fetches an aggregation from the DB and returns it
func GetAggregation(id string) (Aggregation, error) {
	cmd := Redis.HGet(aggrKeyPrefix+id, "Limit")
	err := cmd.Err()
	if err != nil {
		return Aggregation{}, err
	}
	maxConns, err := strconv.Atoi(cmd.Val())
	if err != nil {
		return Aggregation{}, err
	}
	return Aggregation{ID: id, Limit: maxConns}, nil
}

// Save updates/ creates the current aggregation in redis.
func (aggr *Aggregation) Save() error {
	cmd := Redis.HSet(aggrKeyPrefix+aggr.ID, "Limit", aggr.Limit)
	return cmd.Err()
}

// Exists checks if the given aggregation exists in the DB
func (aggr *Aggregation) Exists() (bool, error) {
	res, err := Redis.Exists(aggrKeyPrefix + aggr.ID).Result()

	if err != nil {
		return false, err
	}

	return res > 0, nil
}
