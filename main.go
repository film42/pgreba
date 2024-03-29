package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/film42/pgreba/config"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

type HealthCheckWebService struct {
	healthChecker *HealthChecker
}

func (hc *HealthCheckWebService) apiGetIsPrimary(w http.ResponseWriter, r *http.Request) {
	nodeInfo, err := hc.healthChecker.dataSource.GetNodeInfo()
	if err != nil {
		// Return a 500. Something bad happened.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !nodeInfo.IsPrimary() {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(nodeInfo)
}

func (hc *HealthCheckWebService) apiGetIsReplica(w http.ResponseWriter, r *http.Request) {
	nodeInfo, err := hc.healthChecker.dataSource.GetNodeInfo()
	if err != nil {
		// Return a 500. Something bad happened.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// if replica OR byte lag exceeds max_allowable_byte_lag then return 500
	if !nodeInfo.IsReplica() || maxAllowableByteLagExceeded(r, nodeInfo) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(nodeInfo)
}

func maxAllowableByteLagExceeded(r *http.Request, nodeInfo *NodeInfo) bool {
	maxAllowableByteLagString := r.URL.Query().Get("max_allowable_byte_lag")

	// If byte lag was not specified, assume the replica is up to date.
	if len(maxAllowableByteLagString) == 0 {
		return false
	}

	maxAllowableByteLag, err := strconv.ParseInt(maxAllowableByteLagString, 10, 64)
	if err != nil {
		panic(err)
	}

	return nodeInfo.ByteLag > maxAllowableByteLag
}

func main() {
	versionPtr := flag.Bool("version", false, "Print the teecp version and exit.")
	flag.Parse()

	if *versionPtr {
		fmt.Println("1.3.2")
		return
	}

	if len(os.Args) < 2 {
		panic(errors.New("Please provide a path to config yml."))
	}
	pathToConfig := os.Args[1]

	cfg, err := config.ParseConfig(pathToConfig)

	if err != nil {
		panic(err)
	}

	ds := NewPgReplicationDataSource(cfg)
	defer ds.Close()

	// Wrap the data source in a caching layer to prevent
	// many concurrent health-checks from bogging things down.
	ds = NewCachedDataSource(ds)

	// Fake data source
	fds := new(fakeDataSource)
	fds = fds

	hc := NewHealthChecker(ds)
	hcs := &HealthCheckWebService{healthChecker: hc}

	router := mux.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return handlers.LoggingHandler(log.Writer(), next)
	})

	router.HandleFunc("/", hcs.apiGetIsPrimary).Methods("GET")
	router.HandleFunc("/primary", hcs.apiGetIsPrimary).Methods("GET")

	// For replicas
	router.HandleFunc("/replica", hcs.apiGetIsReplica).Methods("GET")

	log.Println("Listening on :8000")
	http.ListenAndServe(":8000", router)
}
