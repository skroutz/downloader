package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.skroutz.gr/skroutz/downloader/job"
	"golang.skroutz.gr/skroutz/downloader/storage"
)

type API struct {
	Server  *http.Server
	Storage *storage.Storage
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

func New(s *storage.Storage, host string, port int, heartbeatPath string) *API {
	as := &API{Storage: s}
	mux := http.NewServeMux()
	mux.Handle("/download", as)
	mux.HandleFunc("/hb", heartbeat(heartbeatPath))
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
		return
	}
	r.Body.Close()

	j := new(job.Job)
	err = json.Unmarshal(body, j)
	if err != nil {
		http.Error(w, "Error parsing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	foundJID := false
	for i := 0; i < 3; i++ {
		j.ID = idgen.rand()
		exists, err := as.Storage.JobExists(j)
		if err != nil {
			http.Error(w, "Error queuing download: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if !exists {
			foundJID = true
			break
		}
	}
	if !foundJID {
		http.Error(w, "Could not find unique ID after 3 tries. ID: "+j.ID,
			http.StatusInternalServerError)
		return
	}

	aggr := new(job.Aggregation)
	err = json.Unmarshal(body, aggr)
	if err != nil {
		http.Error(w, "Error parsing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	// TODO: do we want to throw error or override the previous aggr?
	exists, err := as.Storage.AggregationExists(aggr)
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
