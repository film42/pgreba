package main

import (
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"gopkg.in/volatiletech/null.v6"
)

// Postgres repication data models

type PgReplicationSlot struct {
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

type PgStatReplication struct {
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

func (sr *PgStatReplication) LagFromUpstream() time.Duration {
	// NOTE: Do we want to use replay lag here?
	return sr.FlushLag
}

// Generic type useful for mocking out the health checking logic.
type ReplicationDataSource interface {
	GetPgStatReplication() ([]*PgStatReplication, error)
	GetPgReplicationSlots() ([]*PgReplicationSlot, error)
	Close() error
}

// Postgres connection impl of replication data source.
type pgReplicationDataSource struct {
	DB *sqlx.DB
}

func NewPgReplicationDataSource(connInfo string) (ReplicationDataSource, error) {
	db, err := sqlx.Connect("postgres", connInfo)
	if err != nil {
		return nil, err
	}

	return &pgReplicationDataSource{DB: db}, nil
}

func (ds *pgReplicationDataSource) Close() error {
	return ds.DB.Close()
}

func (ds *pgReplicationDataSource) GetPgStatReplication() ([]*PgStatReplication, error) {
	stats := []*PgStatReplication{}
	// TODO: Make this only grab required fields.
	err := ds.DB.Select(&stats, "select * from pg_stat_replication;")
	return stats, err
}

func (ds *pgReplicationDataSource) GetPgReplicationSlots() ([]*PgReplicationSlot, error) {
	slots := []*PgReplicationSlot{}
	// TODO: Make this only grab required fields.
	err := ds.DB.Select(&slots, "select * from pg_replication_slots;")
	return slots, err
}
