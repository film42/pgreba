package main

import (
	"encoding/json"
	"log"
	"net/http"

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
	nodeInfo, err := hc.healthChecker.dataSource.GetNodeInfo()
	if err != nil {
		// Return a 500. Something bad happened.
		panic(err)
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
		panic(err)
	}

	if !nodeInfo.IsReplica() {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(nodeInfo)
}

func main() {
	cfg, err := config.ParseConfig("./examples/config.yml")
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

	// For primary nodes
	router.HandleFunc("/", hcs.apiGetIsPrimary).Methods("GET")
	router.HandleFunc("/primary", hcs.apiGetIsPrimary).Methods("GET")

	// For replicas
	router.HandleFunc("/replica", hcs.apiGetIsReplica).Methods("GET")

	log.Println("Listening on :8000")
	http.ListenAndServe(":8000", router)
}
