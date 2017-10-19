package api

import (
	"encoding/json"
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
	decoder := json.NewDecoder(r.Body)
	j := new(job.Job)
	err := decoder.Decode(j)
	if err != nil {
		http.Error(w, "Error converting results to json",
			http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

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

	// TODO: limit should be given from the request instead
	aggr, err := job.NewAggregation(j.AggrID, 8)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
