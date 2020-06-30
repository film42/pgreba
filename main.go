package main

import (
	_ "github.com/lib/pq"

	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	"gopkg.in/volatiletech/null.v6"
)

// NOTES:
// A healthy database replica is:
// 1. Online and accepting connections.
// 2. Is actively replicating from the upstream DB.
// 3. Has a lag of <= 1 second.

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

type pgStatReplication struct {
	Pid             string        `db:"pid"`
	UseSysPid       string        `db:"usesysid"`
	UseName         string        `db:"usename"`
	ApplicationName string        `db:"application_name"`
	ClientAddr      string        `db:"client_addr"`
	ClientHostName  string        `db:"client_hostname"`
	ClientPort      string        `db:"client_port"`
	BackendStart    string        `db:"backend_start"`
	BackendXMin     string        `db:"backend_xmin"`
	State           string        `db:"state"`
	SentLsn         string        `db:"sent_lsn"`
	WriteLsn        string        `db:"write_lsn"`
	FlushLsn        string        `db:"flush_lsn"`
	ReplayLsn       string        `db:"replay_lsn"`
	WriteLag        time.Duration `db:"write_lag"`
	FlushLag        time.Duration `db:"flush_lag"`
	ReplayLag       time.Duration `db:"replay_lag"`
	SyncPriority    string        `db:"sync_priority"`
	SyncState       string        `db:"sync_state"`
	ReplyTime       string        `db:"reply_time"`
}

func (sr *pgStatReplication) LagFromUpstream() time.Duration {
	// NOTE: Do we want to use replay lag here?
	return sr.FlushLag
}

type ReplicationChecker interface {
	GetPgStatReplication() ([]*pgStatReplication, error)
	GetPgReplicationSlots() ([]*pgReplicationSlot, error)
}

type dbReplication struct {
	DB *sqlx.DB
}

func (dr *dbReplication) GetPgStatReplication() ([]*pgStatReplication, error) {
	stats := []*pgStatReplication{}
	err := dr.DB.Select(&stats, "select * from pg_stat_replication;")
	return stats, err
}

func (dr *dbReplication) GetPgReplicationSlots() ([]*pgReplicationSlot, error) {
	// Get all replication slots
	slots := []*pgReplicationSlot{}
	err := dr.DB.Select(&slots, "select * from pg_replication_slots;")
	return slots, err
}

type HealthChecker struct {
	replicationChecker ReplicationChecker
}

func (hc *HealthChecker) getSlotByName(name string) (*pgReplicationSlot, error) {
	slots, err := hc.replicationChecker.GetPgReplicationSlots()
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

func (hc *HealthChecker) getStatReplication(name string) (*pgStatReplication, error) {
	stats, err := hc.replicationChecker.GetPgStatReplication()
	if err != nil {
		return nil, err
	}

	for _, stat := range stats {
		if stat.ApplicationName == name {
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

	statReplication, err := hc.getStatReplication(slotName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Println("ERR:", err)
		return
	}

	// If stat replication is missing return 404
	if statReplication == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// If the lag is > 1 second, it's unhealthy.
	if statReplication.LagFromUpstream() > time.Second {
		w.WriteHeader(http.StatusServiceUnavailable)
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

type fakeDbReplication struct{}

func (fdr *fakeDbReplication) GetPgStatReplication() ([]*pgStatReplication, error) {
	return []*pgStatReplication{
		{
			ApplicationName: "pghost_created_replication_slot",
			FlushLag:        time.Second,
		},
	}, nil
}
func (fdr *fakeDbReplication) GetPgReplicationSlots() ([]*pgReplicationSlot, error) {
	return []*pgReplicationSlot{
		{
			SlotName: "pghost_created_replication_slot",
			Active:   true,
		},
	}, nil
}

func main() {
	fmt.Println("Hello")
	db, err := sqlx.Connect("postgres", "host=localhost database=postgres user=postgres sslmode=disable binary_parameters=yes")
	if err != nil {
		panic(err)
	}

	fdr := new(fakeDbReplication)
	fdr = fdr

	dr := &dbReplication{DB: db}
	dr = dr

	hc := &HealthChecker{replicationChecker: fdr}

	router := mux.NewRouter()
	router.HandleFunc("/slot/{slot_name}/health_check", hc.getSlotHealthCheck).Methods("GET")
	http.ListenAndServe(":8000", router)
}
