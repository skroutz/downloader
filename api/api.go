package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

type API struct {
	Server  *http.Server
	Storage *storage.Storage
}

func New(s *storage.Storage, host string, port int) *API {
	as := &API{Storage: s}
	mux := http.NewServeMux()
	mux.Handle("/download", as)
	as.Server = &http.Server{Handler: mux, Addr: host + ":" + strconv.Itoa(port)}
	return as
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
	}
	r.Body.Close()

	j := new(job.Job)
	err = json.Unmarshal(body, j)
	if err != nil {
		http.Error(w, "Error parsing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	exists, err := as.Storage.JobExists(j)
	if err != nil {
		http.Error(w, "Error queuing download: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	if exists {
		http.Error(w, "Job with provided id already exists",
			http.StatusBadRequest)
		return
	}

	aggr := new(job.Aggregation)
	err = json.Unmarshal(body, aggr)
	if err != nil {
		http.Error(w, "Error parsing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	exists, err = as.Storage.AggregationExists(aggr)
	if err != nil {
		http.Error(w, "Error queuing download: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	if !exists {
		err = as.Storage.SaveAggregation(aggr)
		if err != nil {
			http.Error(w, "Error queuing download: "+err.Error(),
				http.StatusInternalServerError)
			return
		}
	}

	if err = as.Storage.QueuePendingDownload(j); err != nil {
		http.Error(w, "Error queuing download: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}
