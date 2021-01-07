package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/film42/pgreba/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"gopkg.in/volatiletech/null.v6"
)

func sqlConnect(connInfo string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", connInfo)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(time.Second * 5)
	db.SetConnMaxLifetime(time.Second * 5)
	return db, nil
}

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

type PgStatWalReceiver struct {
	Pid                string `db:"pid"`
	Status             string `db:"status"`
	ReceivedLsn        string `db:"received_lsn"`
	ReceivedTli        string `db:"received_tli"`
	ReceiveStartLsn    string `db:"receive_start_lsn"`
	ReceiveStartTli    string `db:"receive_start_tli"`
	LastMsgSendTime    string `db:"last_msg_send_time"`
	LastMsgReceiptTime string `db:"last_msg_receipt_time"`
	LatestEndLsn       string `db:"latest_end_lsn"`
	LatestEndTime      string `db:"latest_end_time"`
	SlotName           string `db:"slot_name"`
	ConnInfo           string `db:"conninfo"`
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

type XlogInfo struct {
	Location          int64       `json:"location"`
	ReceivedLocation  int64       `json:"received_location"`
	ReplayedLocation  null.Int64  `json:"replayed_location"`
	ReplayedTimestamp null.String `json:"replayed_timestamp"`
	Paused            bool        `json:"paused"`
}

type ReplicationInfo struct {
	Username        string `json:"username"`
	ApplicationName string `json:"application_name"`
	ClientAddr      string `json:"client_addr"`
	State           string `json:"state"`
	SyncState       string `json:"sync_state"`
	SyncPriority    int64  `json:"sync_priority"`
}

type NodeInfo struct {
	State               int64              `json:"state"`
	PostmasterStartTime string             `json:"postmaster_start_time"`
	Role                string             `json:"role"`
	Xlog                *XlogInfo          `json:"xlog"`
	Replication         []*ReplicationInfo `json:"replication"`
	ByteLag             int64              `json:"byte_lag"`
}

func (ni *NodeInfo) IsPrimary() bool {
	return ni.Role == "primary"
}

func (ni *NodeInfo) IsReplica() bool {
	return ni.Role == "replica"
}

func (sr *PgStatReplication) LagFromUpstream() time.Duration {
	// NOTE: Do we want to use replay lag here?
	return sr.FlushLag
}

// Generic type useful for mocking out the health checking logic.
type ReplicationDataSource interface {
	GetNodeInfo() (*NodeInfo, error)
	IsInRecovery() (bool, error)
	GetPgStatReplication() ([]*PgStatReplication, error)
	GetPgReplicationSlots() ([]*PgReplicationSlot, error)
	Close() error
}

// Postgres connection impl of replication data source.
type pgDataSource struct {
	db      *sqlx.DB
	cfg     *config.Config
	dbMutex sync.Mutex
}

func NewPgReplicationDataSource(config *config.Config) ReplicationDataSource {
	return &pgDataSource{cfg: config, dbMutex: sync.Mutex{}}
}

func (ds *pgDataSource) Close() error {
	ds.dbMutex.Lock()
	defer ds.dbMutex.Unlock()
	if ds.db == nil {
		return nil
	}
	err := ds.db.Close()
	ds.db = nil
	return err
}

func (ds *pgDataSource) getDB() (*sqlx.DB, error) {
	ds.dbMutex.Lock()
	defer ds.dbMutex.Unlock()
	if ds.db != nil {
		return ds.db, nil
	}
	db, err := sqlConnect(fmt.Sprintf("host=%s port=%s database=%s user=%s sslmode=%s binary_parameters=%s", ds.cfg.Host, ds.cfg.Port, ds.cfg.Database, ds.cfg.User, ds.cfg.Sslmode, ds.cfg.BinaryParameters))
	if err != nil {
		fmt.Println("Error creating a connection pool.")
		return nil, err
	}
	ds.db = db

	return db, nil
}

func (ds *pgDataSource) GetNodeInfo() (*NodeInfo, error) {
	// NOTE: This was copied from patroni.
	sql := `
SELECT pg_catalog.to_char(pg_catalog.pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
       CASE
           WHEN pg_catalog.pg_is_in_recovery() THEN 0
           ELSE ('x' || pg_catalog.substr(pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()), 1, 8))::bit(32)::int
       END,
       CASE
           WHEN pg_catalog.pg_is_in_recovery() THEN 0
           ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), '0/0')::bigint
       END,
       pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(), '0/0')::bigint,
       pg_catalog.pg_wal_lsn_diff(COALESCE(pg_catalog.pg_last_wal_receive_lsn(), '0/0'), '0/0')::bigint,
       pg_catalog.pg_is_in_recovery()
AND pg_catalog.pg_is_wal_replay_paused(),
    pg_catalog.to_char(pg_catalog.pg_last_xact_replay_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
    pg_catalog.array_to_json(pg_catalog.array_agg(pg_catalog.row_to_json(ri)))
FROM
  (SELECT
     (SELECT rolname
      FROM pg_authid
      WHERE oid = usesysid) AS usename,
          application_name,
          client_addr,
          w.state,
          sync_state,
          sync_priority
   FROM pg_catalog.pg_stat_get_wal_senders() w,
        pg_catalog.pg_stat_get_activity(pid)) AS ri
`
	db, dbErr := ds.getDB()
	if dbErr != nil {
		return nil, dbErr
	}

	rows, err := db.Queryx(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("err: did not find at least one row in node info response")
	}

	// Parse out results from DB
	var replicationSummary []byte
	nodeInfo := &NodeInfo{
		Xlog:        &XlogInfo{},
		Replication: []*ReplicationInfo{},
	}
	err = rows.Scan(
		&nodeInfo.PostmasterStartTime,
		&nodeInfo.State,
		&nodeInfo.Xlog.Location,
		&nodeInfo.Xlog.ReplayedLocation,
		&nodeInfo.Xlog.ReceivedLocation,
		&nodeInfo.Xlog.Paused,
		&nodeInfo.Xlog.ReplayedTimestamp,
		&replicationSummary,
	)
	if err != nil {
		return nil, err
	}

	// Parse out the replication summary if present.
	if len(replicationSummary) > 0 {
		err = json.Unmarshal(replicationSummary, &nodeInfo.Replication)
		if err != nil {
			return nil, err
		}
	}

	// Make patroni api tweaks
	if nodeInfo.State == 0 {
		nodeInfo.Role = "replica"
	} else {
		nodeInfo.Role = "primary"
	}
	if nodeInfo.Xlog.ReceivedLocation == 0 {
		nodeInfo.Xlog.ReceivedLocation = nodeInfo.Xlog.ReplayedLocation.Int64
	}

	pgCurrentWalLsn, err := ds.getPgCurrentWalLsn(ds.cfg.MaxHop, db)
	if err != nil {
		log.Fatalln("Error getting pg_current_wal_lsn:", err)
	}

	pgLastWalLsn, err := ds.getPgLastWalReplayLsn()
	if err != nil {
		log.Fatalln("Error getting pg_last_wal_replay_lsn:", err)
	}
	// Skip the byte lag checks if the last wal lsn is empty
	if pgLastWalLsn == "" {
		return nodeInfo, nil
	}

	byteLag, err := ds.getPgWalLsnDiff(pgCurrentWalLsn, pgLastWalLsn)
	if err != nil {
		log.Fatalln("Error getting pg_wal_lsn_diff:", err)
	}

	nodeInfo.ByteLag = byteLag

	return nodeInfo, nil
}

func (ds *pgDataSource) getUpstreamConnInfo(db *sqlx.DB) (string, error) {
	stats := PgStatWalReceiver{}
	err := db.Get(&stats, "select * from pg_stat_wal_receiver;")
	if err != nil {
		return "", err
	}
	return stats.ConnInfo, nil
}

func parseConnInfo(conninfo string) map[string]string {
	params := strings.Split(conninfo, " ")

	parsedConnInfo := make(map[string]string)
	for _, param := range params {
		kv := strings.Split(param, "=")
		parsedConnInfo[kv[0]] = kv[1]
	}
	return parsedConnInfo
}

func (ds *pgDataSource) buildConnInfo(conninfo map[string]string) string {
	return fmt.Sprintf("host=%s port=%s database=%s user=%s sslmode=%s binary_parameters=%s password=%s",
		conninfo["host"], conninfo["port"],
		ds.cfg.Database, ds.cfg.User, ds.cfg.Sslmode, ds.cfg.BinaryParameters, ds.cfg.Password)
}

func (ds *pgDataSource) getPgCurrentWalLsn(maxHop int64, db *sqlx.DB) (string, error) {
	var isReplica bool
	err := db.Get(&isReplica, "select pg_catalog.pg_is_in_recovery()")
	defer db.Close()
	if err != nil {
		return "", err
	}

	if isReplica {
		if maxHop == 0 {
			return "", errors.New("Reached max hop limit")
		}

		conninfo, err := ds.getUpstreamConnInfo(db)
		if err != nil {
			return "", err
		}
		upstreamConnInfo := ds.buildConnInfo(parseConnInfo(conninfo))
		upstreamDb, err := sqlConnect(upstreamConnInfo)
		return ds.getPgCurrentWalLsn(maxHop-1, upstreamDb)
	}

	var pgCurrentWalLsn string
	err = db.Get(&pgCurrentWalLsn, "select pg_current_wal_lsn()")
	if err != nil {
		return "", err
	}
	return pgCurrentWalLsn, nil
}

func (ds *pgDataSource) getPgLastWalReplayLsn() (string, error) {
	db, dbErr := ds.getDB()
	if dbErr != nil {
		return "", dbErr
	}

	pgLastWalLsn := null.String{}
	err := db.Get(&pgLastWalLsn, "select pg_last_wal_replay_lsn()")
	if err != nil {
		return "", err
	}
	return pgLastWalLsn.String, nil
}

func (ds *pgDataSource) getPgWalLsnDiff(currentLsn string, lastLsn string) (int64, error) {
	db, dbErr := ds.getDB()
	if dbErr != nil {
		return 0, dbErr
	}

	var byteLag int64

	query := fmt.Sprintf("select pg_wal_lsn_diff('%s', '%s')", currentLsn, lastLsn)

	err := db.Get(&byteLag, query)
	if err != nil {
		return 0, err
	}
	return byteLag, nil
}

func (ds *pgDataSource) IsInRecovery() (bool, error) {
	db, dbErr := ds.getDB()
	if dbErr != nil {
		return false, dbErr
	}

	var isInRecovery bool

	err := db.Get(&isInRecovery, "select pg_catalog.pg_is_in_recovery()")
	return isInRecovery, err
}

func (ds *pgDataSource) GetPgStatReplication() ([]*PgStatReplication, error) {
	stats := []*PgStatReplication{}
	// TODO: Make this only grab required fields.
	db, dbErr := ds.getDB()
	if dbErr != nil {
		return nil, dbErr
	}

	err := db.Select(&stats, "select * from pg_stat_replication")
	return stats, err
}

func (ds *pgDataSource) GetPgReplicationSlots() ([]*PgReplicationSlot, error) {
	slots := []*PgReplicationSlot{}
	// TODO: Make this only grab required fields.
	db, dbErr := ds.getDB()
	if dbErr != nil {
		return nil, dbErr
	}

	err := db.Select(&slots, "select * from pg_replication_slots")
	return slots, err
}

// Caching data source for efficient lookup

type cachedDataSource struct {
	dataSource ReplicationDataSource
	mutex      sync.Mutex
	cacheTTL   time.Duration

	cachedGetNodeInfo          *NodeInfo
	cachedGetNodeInfoExpiresAt time.Time

	cachedIsInRecovery          bool
	cachedIsInRecoveryExpiresAt time.Time

	cachedGetPgStatReplication          []*PgStatReplication
	cachedGetPgStatReplicationExpiresAt time.Time

	cachedGetPgReplicationSlots          []*PgReplicationSlot
	cachedGetPgReplicationSlotsExpiresAt time.Time
}

func NewCachedDataSource(ds ReplicationDataSource) ReplicationDataSource {
	return &cachedDataSource{dataSource: ds, mutex: sync.Mutex{}, cacheTTL: time.Second}
}

func (ds *cachedDataSource) GetNodeInfo() (*NodeInfo, error) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// If the cache has expired.
	if ds.cachedGetNodeInfoExpiresAt.Before(time.Now()) {
		var err error
		ds.cachedGetNodeInfo, err = ds.dataSource.GetNodeInfo()
		if err != nil {
			return nil, err
		}

		// Increase ttl point because result was valid
		ds.cachedGetNodeInfoExpiresAt = time.Now().Add(ds.cacheTTL)
	}

	return ds.cachedGetNodeInfo, nil
}

func (ds *cachedDataSource) IsInRecovery() (bool, error) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// If the cache has expired.
	if ds.cachedIsInRecoveryExpiresAt.Before(time.Now()) {
		var err error
		ds.cachedIsInRecovery, err = ds.dataSource.IsInRecovery()
		if err != nil {
			return false, err
		}

		// Increase ttl point because result was valid
		ds.cachedIsInRecoveryExpiresAt = time.Now().Add(ds.cacheTTL)
	}

	return ds.cachedIsInRecovery, nil
}

func (ds *cachedDataSource) GetPgStatReplication() ([]*PgStatReplication, error) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// If the cache has expired.
	if ds.cachedGetPgStatReplicationExpiresAt.Before(time.Now()) {
		var err error
		ds.cachedGetPgStatReplication, err = ds.dataSource.GetPgStatReplication()
		if err != nil {
			return nil, err
		}

		// Increase ttl point because result was valid
		ds.cachedGetPgStatReplicationExpiresAt = time.Now().Add(ds.cacheTTL)
	}

	return ds.cachedGetPgStatReplication, nil
}

func (ds *cachedDataSource) GetPgReplicationSlots() ([]*PgReplicationSlot, error) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// If the cache has expired.
	if ds.cachedGetPgReplicationSlotsExpiresAt.Before(time.Now()) {
		var err error
		ds.cachedGetPgReplicationSlots, err = ds.dataSource.GetPgReplicationSlots()
		if err != nil {
			return nil, err
		}

		// Increase ttl point because result was valid
		ds.cachedGetPgReplicationSlotsExpiresAt = time.Now().Add(ds.cacheTTL)
	}

	return ds.cachedGetPgReplicationSlots, nil
}

func (ds *cachedDataSource) Close() error {
	return ds.dataSource.Close()
}
