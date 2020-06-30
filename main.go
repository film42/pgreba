package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

type HealthCheckWebService struct {
	healthChecker *HealthChecker
}

func (hc *HealthCheckWebService) getSlotHealthCheck(w http.ResponseWriter, r *http.Request) {
	// Get request info
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	slotName := params["slot_name"]

	// Perform the health check.
	err := hc.healthChecker.CheckReplicationSlot(slotName)

	// If the slot is OK, return status: ok.
	if err == nil {
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"slot":   slotName,
		})
		return
	}

	// If there was an error, set the appropriate status code.
	switch err {
	case ErrReplicationSlotNotFound:
		w.WriteHeader(http.StatusNotFound)
	case ErrReplicationSlotLagTooHigh:
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}

	// Return error to the client.
	json.NewEncoder(w).Encode(map[string]string{
		"error": err.Error(),
		"slot":  slotName,
	})
}

var (
	defaultConnInfo = "host=localhost database=postgres user=postgres sslmode=disable binary_parameters=yes"
)

func main() {
	ds, err := NewPgReplicationDataSource(defaultConnInfo)
	if err != nil {
		panic(err)
	}
	defer ds.Close()

	// Fake data source
	fds := new(fakeDataSource)
	fds = fds

	hc := NewHealthChecker(fds)
	hcs := &HealthCheckWebService{healthChecker: hc}

	router := mux.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return handlers.LoggingHandler(log.Writer(), next)
	})
	router.HandleFunc("/slot/{slot_name}/health_check", hcs.getSlotHealthCheck).Methods("GET")

	log.Println("Listening on :8000")
	http.ListenAndServe(":8000", router)
}
