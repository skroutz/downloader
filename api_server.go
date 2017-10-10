package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func NewAPIServer(host string, port int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/download/", handleDownload)
	return &http.Server{Handler: mux, Addr: host + ":" + strconv.Itoa(port)}
}

// handleDownload enqueues new downloads to the backend Redis instance
func handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
	decoder := json.NewDecoder(r.Body)
	j := Job{}
	err := decoder.Decode(&j)
	if err != nil {
		http.Error(w, "Error converting results to json",
			http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	exists, err := j.Exists()
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

	aggr := Aggregation{ID: j.AggrID}
	exists, err = aggr.Exists()
	if err != nil {
		http.Error(w, "Error queuing download: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	if !exists {
		err = aggr.Save()
		if err != nil {
			http.Error(w, "Error queuing download: "+err.Error(),
				http.StatusInternalServerError)
			return
		}
	}

	if err = j.QueuePendingDownload(); err != nil {
		http.Error(w, "Error queuing download: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}
