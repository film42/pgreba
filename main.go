package main

import (
	_ "github.com/lib/pq"

	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	"gopkg.in/volatiletech/null.v6"
)

type pgReplicationSlot struct {
	SlotName          string      `db:"slot_name"`
	Plugin            string      `db:"plugin"`
	SlotType          string      `db:"slot_type"`
	Datoid            string      `db:"datoid"`
	Database          string      `db:"database"`
	Temporary         bool        `db:"temporary"`
	Active            bool        `db:"active"`
	ActivePid         null.String `db:"active_pid"`
	Xmin              null.String `db:"xmin"`
	CatalogXmin       string      `db:"catalog_xmin"`
	RestartLsn        string      `db:"restart_lsn"`
	ConfirmedFlushLsn string      `db:"confirmed_flush_lsn"`
}

type pgReplicationStat struct {
  ApplicationName   string      `db:"application_name"`
  FlushLag          string      `db:"flush_lag"`
  ReplayLag         string      `db:"replay_lag"`
}

type HealthChecker struct {
	DB *sqlx.DB
}

func (hc *HealthChecker) getSlotByName(name string) (*pgReplicationSlot, error) {
	// Get all replication slots
	slots := []*pgReplicationSlot{}
	err := hc.DB.Select(&slots, "select * from pg_replication_slots;")
	if err != nil {
		return nil, err
	}

	for _, slot := range slots {
		if slot.SlotName == name {
			return slot, nil
		}
	}

	return nil, nil
}

func (hc *HealthChecker) getReplicationStat(name string) (*pgReplcationStat, error) {
  // Get Replication Stat
  stats := []*pgReplcationStat{}
  err := hc.DB.Select(&stats, "select * from pg_replication_stats;")
  if err != nil {
    return nil, err
  }

  for _, stat := range stats {
    if stat.ApplicatioName == name {
      return stat, nil
    }
  }

  return nil, nil
}

func (hc *HealthChecker) getSlotHealthCheck(w http.ResponseWriter, r *http.Request) {
	// Get request info
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	slotName := params["slot_name"]

	// Get slot
	slot, err := hc.getSlotByName(slotName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Println("ERR:", err)
		return
	}

	// If slot is missing return 404
	if slot == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// If slot is not active return 503
	if !slot.Active {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	// Render the response
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active": slot.Active,
		"slot":   slotName,
	})
}

func main() {
	fmt.Println("Hello")
	db, err := sqlx.Connect("postgres", "host=localhost database=postgres user=postgres sslmode=disable binary_parameters=yes")
	if err != nil {
		panic(err)
	}

	hc := &HealthChecker{DB: db}

	router := mux.NewRouter()
	router.HandleFunc("/slot/{slot_name}/health_check", hc.getSlotHealthCheck).Methods("GET")
	http.ListenAndServe(":8000", router)
}
