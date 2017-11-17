package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

type API struct {
	Server  *http.Server
	Storage *storage.Storage
	Log     *log.Logger
}

var idgen *rng

func init() {
	idgen = newRNG(10,
		rand.NewSource(time.Now().UnixNano()),
		base64.RawURLEncoding)
}

// heartbeat returns http.statusServiceUnavailable (503) if path exists, 200 otherwise
func heartbeat(path string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "OK")
			return
		}

		msg := fmt.Sprintf("Service disabled, '%s' exists!", path)
		http.Error(w, msg, http.StatusServiceUnavailable)
	}
}

func (as *API) stats(w http.ResponseWriter, r *http.Request) {
	id := path.Base(r.RequestURI)

	respbytes, err := as.Storage.GetStats(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if respbytes == nil {
		http.Error(w, fmt.Sprintf("Could not find stats for entity %s", id), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(respbytes)
	if err != nil {
		as.Log.Println("Error reporting stats:", err)
	}
}

func (as *API) retry(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	id := path.Base(r.URL.Path)
	j, err := as.Storage.GetJob(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching %s from Redis: %s", id, err),
			http.StatusBadRequest)
		return
	}

	if j.CallbackState != job.StateFailed {
		http.Error(w, fmt.Sprintf("Error with callback state of the job %s: %s", j, j.CallbackState),
			http.StatusBadRequest)
		return
	}

	err = as.Storage.RetryCallback(&j)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error rescheduling callback for job with id '%s': %s", id, err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func New(s *storage.Storage, host string, port int, heartbeatPath string,
	logger *log.Logger) *API {
	as := &API{Storage: s}
	mux := http.NewServeMux()
	mux.Handle("/download", as)
	mux.HandleFunc("/hb", heartbeat(heartbeatPath))
	mux.HandleFunc("/stats/", as.stats)
	mux.HandleFunc("/retry/", as.retry)
	mux.HandleFunc("/dashboard/aggregations", as.dashboardAggregations)
	as.Server = &http.Server{Handler: mux, Addr: host + ":" + strconv.Itoa(port)}
	as.Log = logger
	return as
}

// apiAggregations returns a JSON list of aggregations with pending jobs.
func (as *API) dashboardAggregations(w http.ResponseWriter, r *http.Request) {
	type aggr struct {
		Name string `json:"name"`
		Size int64  `json:"size"`
	}
	resp := make([]aggr, 0)

	iter := as.Storage.Redis.Scan(0, storage.JobsKeyPrefix+"*", 0).Iterator()

	for iter.Next() {
		a := iter.Val()
		count, err := as.Storage.Redis.ZCount(a, "-inf", "+inf").Result()
		if err != nil {
			count = -1
		}
		resp = append(resp, aggr{a, count})
	}

	if err := iter.Err(); err != nil {
		http.Error(w, fmt.Sprintf("Error scanning for aggregation keys: %v", err), http.StatusInternalServerError)
		return
	}

	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error marshaling json: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if _, err := w.Write(body); err != nil {
		as.Log.Print("Error writing response: ", err)
	}
}

// ServeHTTP enqueues new downloads to the backend Redis instance
func (as *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	j := new(job.Job)
	err = json.Unmarshal(body, j)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error unmarshalling body '%s' to Job: %s", body, err),
			http.StatusBadRequest)
		return
	}

	foundJID := false
	for i := 0; i < 3; i++ {
		j.ID = idgen.rand()
		exists, err := as.Storage.JobExists(j)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error fetching %s from Redis: %s", j, err),
				http.StatusInternalServerError)
			return
		}
		if !exists {
			foundJID = true
			break
		}
	}
	if !foundJID {
		http.Error(w, fmt.Sprintf("Could not find unique ID after 3 tries for %s", j),
			http.StatusInternalServerError)
		return
	}

	aggr := new(job.Aggregation)
	err = json.Unmarshal(body, aggr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error unmarshalling body '%s' to Aggregation: %s", body, err),
			http.StatusBadRequest)
		return
	}

	// TODO: do we want to throw error or override the previous aggr?
	exists, err := as.Storage.AggregationExists(aggr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching aggregation for %s: %s", j, err),
			http.StatusInternalServerError)
		return
	}
	if !exists {
		err = as.Storage.SaveAggregation(aggr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error persisting aggregation for %s: %s", j, err),
				http.StatusInternalServerError)
			return
		}
		as.Log.Printf("Created aggregation %v for %s", *aggr, j)
	}

	err = as.Storage.QueuePendingDownload(j, 0)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error queueing %s: %s", j, err),
			http.StatusInternalServerError)
		return
	}
	as.Log.Println("Enqueued", j)

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")

	resp := fmt.Sprintf(`{"id":"%s"}`, j.ID)
	_, err = w.Write([]byte(resp))
	if err != nil {
		as.Log.Printf("Error writing response for %s: %s", j, err)
	}
}
