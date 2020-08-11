package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/film42/pgreba/config"
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
	Conninfo           string `db:"conninfo"`
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
	ReplayedLocation  int64       `json:"replayed_location"`
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
	DB *sqlx.DB
}

func NewPgReplicationDataSource(connInfo string) (ReplicationDataSource, error) {
	db, err := sqlx.Connect("postgres", connInfo)
	if err != nil {
		return nil, err
	}

	return &pgDataSource{DB: db}, nil
}

func (ds *pgDataSource) Close() error {
	return ds.DB.Close()
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
	rows, err := ds.DB.Queryx(sql)
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
		nodeInfo.Xlog.ReceivedLocation = nodeInfo.Xlog.ReplayedLocation
	}

	pgCurrentWalLsn, err := ds.getPgCurrentWalLsn(nodeInfo.Role)
	if err != nil {
		log.Fatalln("Error getting pg_current_wal_lsn:", err)
	}

	pgLastWalLsn, err := ds.getPgLastWalReceiveLsn()
	if err != nil {
		log.Fatalln("Error getting pg_last_wal_lsn:", err)
	}

	byteLag, err := ds.getPgWalLsnDiff(pgCurrentWalLsn, pgLastWalLsn)
	if err != nil {
		log.Fatalln("Error getting pg_wal_lsn_diff:", err)
	}

	nodeInfo.ByteLag = byteLag

	return nodeInfo, nil
}

func (ds *pgDataSource) getConnInfo() (string, error) {
	stats := PgStatWalReceiver{}
	err := ds.DB.Get(&stats, "select * from pg_stat_wal_receiver;")
	if err != nil {
		return "", err
	}
	return stats.Conninfo, nil
}

func parseConninfo(conninfo string) map[string]string {
	params := strings.Split(conninfo, " ")

	parsedConninfo := make(map[string]string)
	for _, param := range params {
		kv := strings.Split(param, "=")
		parsedConninfo[kv[0]] = kv[1]
	}
	return parsedConninfo
}

func buildConninfo(conninfo map[string]string) string {
	cfg, err := config.ParseConfig("./examples/config.yml")
	if err != nil {
		log.Fatalln("Error parsing config:", err)
	}
	ci := fmt.Sprintf("host=%s port=%s database=%s user=%s sslmode=%s binary_parameters=%s", conninfo["host"], conninfo["port"], cfg.Database, cfg.User, cfg.Sslmode, cfg.BinaryParameters)
	return ci
}

func (ds *pgDataSource) getPgCurrentWalLsn(role string) (string, error) {
	if role == "replica" {
		// query select * from pg_stat_wal_receiver;  to get conninfo
		//create a new db connection to upstream (primary) with conninfo and return pg_current_wal_lsn
		conninfo, err := ds.getConnInfo()
		if err != nil {
			return "", err
		}
		upstreamConnInfo := buildConninfo(parseConninfo(conninfo))
		upstreamDb, err := sqlx.Connect("postgres", upstreamConnInfo)
		var pgCurrentWalLsn string
		err = upstreamDb.Get(&pgCurrentWalLsn, "select pg_current_wal_lsn()")
		if err != nil {
			return "", err
		}
		return pgCurrentWalLsn, nil
	} else {
		var pgCurrentWalLsn string
		err := ds.DB.Get(&pgCurrentWalLsn, "select pg_current_wal_lsn()")
		if err != nil {
			return "", err
		}
		return pgCurrentWalLsn, nil
	}
}

func (ds *pgDataSource) getPgLastWalReceiveLsn() (string, error) {
	var pgLastWalLsn string
	err := ds.DB.Get(&pgLastWalLsn, "select pg_last_wal_receive_lsn()")
	if err != nil {
		return "", err
	}
	return pgLastWalLsn, nil
}

func (ds *pgDataSource) getPgWalLsnDiff(currentLsn string, lastLsn string) (int64, error) {
	var byteLag int64

	query := fmt.Sprintf("select pg_wal_lsn_diff('%s', '%s')", currentLsn, lastLsn)
	err := ds.DB.Get(&byteLag, query)
	if err != nil {
		return 0, err
	}
	return byteLag, nil
}

func (ds *pgDataSource) IsInRecovery() (bool, error) {
	var isInRecovery bool
	err := ds.DB.Get(&isInRecovery, "select pg_catalog.pg_is_in_recovery()")
	return isInRecovery, err
}

func (ds *pgDataSource) GetPgStatReplication() ([]*PgStatReplication, error) {
	stats := []*PgStatReplication{}
	// TODO: Make this only grab required fields.
	err := ds.DB.Select(&stats, "select * from pg_stat_replication")
	return stats, err
}

func (ds *pgDataSource) GetPgReplicationSlots() ([]*PgReplicationSlot, error) {
	slots := []*PgReplicationSlot{}
	// TODO: Make this only grab required fields.
	err := ds.DB.Select(&slots, "select * from pg_replication_slots")
	return slots, err
}
