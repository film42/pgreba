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

// func (hc *HealthCheckWebService) getSlotHealthCheck(w http.ResponseWriter, r *http.Request) {
// 	// Get request info
// 	w.Header().Set("Content-Type", "application/json")
// 	params := mux.Vars(r)
// 	slotName := params["slot_name"]

// 	// Perform the health check.
// 	err := hc.healthChecker.CheckReplicationSlot(slotName)

// 	// If the slot is OK, return status: ok.
// 	if err == nil {
// 		json.NewEncoder(w).Encode(map[string]string{
// 			"status": "ok",
// 			"slot":   slotName,
// 		})
// 		return
// 	}

// 	// If there was an error, set the appropriate status code.
// 	switch err {
// 	case ErrReplicationSlotNotFound:
// 		w.WriteHeader(http.StatusNotFound)
// 	case ErrReplicationSlotLagTooHigh:
// 		w.WriteHeader(http.StatusServiceUnavailable)
// 	default:
// 		w.WriteHeader(http.StatusInternalServerError)
// 	}

// 	// Return error to the client.
// 	json.NewEncoder(w).Encode(map[string]string{
// 		"error": err.Error(),
// 		"slot":  slotName,
// 	})
// }

func (hc *HealthCheckWebService) apiGetIsPrimary(w http.ResponseWriter, r *http.Request) {
	m := r.URL.Query().Get("max_allowable_byte_lag")

	max_allowable_byte_lag := int64(0)

	if len(m) > 0 {
		i, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			log.Fatalln("Error converting query param to int64:", err)
		}
		max_allowable_byte_lag = i
	}

	nodeInfo, err := hc.healthChecker.dataSource.GetNodeInfo()
	if err != nil {
		// Return a 500. Something bad happened.
		panic(err)
	}

	// if byte lag exceeds max_allowable_byte_lag then return 500
	if nodeInfo.ByteLag > max_allowable_byte_lag {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if !nodeInfo.IsPrimary() {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(nodeInfo)
}

func (hc *HealthCheckWebService) apiGetIsReplica(w http.ResponseWriter, r *http.Request) {
	m := r.URL.Query().Get("max_allowable_byte_lag")

	max_allowable_byte_lag := int64(0)

	if len(m) > 0 {
		i, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			log.Fatalln("Error converting query param to int64:", err)
		}
		max_allowable_byte_lag = i
	}

	nodeInfo, err := hc.healthChecker.dataSource.GetNodeInfo()

	if err != nil {
		// Return a 500. Something bad happened.
		panic(err)
	}

	// if byte lag exceeds max_allowable_byte_lag then return 500
	if nodeInfo.ByteLag > max_allowable_byte_lag {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if !nodeInfo.IsReplica() {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(nodeInfo)
}

func main() {
	versionPtr := flag.Bool("version", false, "Print the teecp version and exit.")
	flag.Parse()

	if *versionPtr {
		fmt.Println("1.1.0")
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
	ds, err := NewPgReplicationDataSource(cfg)
	if err != nil {
		panic(err)
	}
	defer ds.Close()

	// Fake data source
	fds := new(fakeDataSource)
	fds = fds

	hc := NewHealthChecker(ds)
	hcs := &HealthCheckWebService{healthChecker: hc}

	router := mux.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return handlers.LoggingHandler(log.Writer(), next)
	})

	router.HandleFunc("/", hcs.apiGetIsPrimary).Queries("max_allowable_byte_lag", "{max_allowable_byte_lag}").Methods("GET")
	router.HandleFunc("/", hcs.apiGetIsPrimary).Methods("GET")
	router.HandleFunc("/primary", hcs.apiGetIsPrimary).Queries("max_allowable_byte_lag", "{max_allowable_byte_lag}").Methods("GET")
	router.HandleFunc("/primary", hcs.apiGetIsPrimary).Methods("GET")

	// For replicas
	router.HandleFunc("/replica", hcs.apiGetIsReplica).Queries("max_allowable_byte_lag", "{max_allowable_byte_lag}").Methods("GET")
	router.HandleFunc("/replica", hcs.apiGetIsReplica).Methods("GET")

	log.Println("Listening on :8000")
	http.ListenAndServe(":8000", router)
}
