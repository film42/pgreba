package main

import (
	"errors"
	"time"
)

var (
	ErrReplicationSlotNotFound   = errors.New("replication slot not found")
	ErrReplicationSlotLagTooHigh = errors.New("replication lag is too high")
)

type HealthChecker struct {
	dataSource ReplicationDataSource
}

func NewHealthChecker(dataSource ReplicationDataSource) *HealthChecker {
	return &HealthChecker{
		dataSource: dataSource,
	}
}

func (hc *HealthChecker) getStatReplicationByName(slotName string) (*PgStatReplication, error) {
	stats, err := hc.dataSource.GetPgStatReplication()
	if err != nil {
		return nil, err
	}

	for _, stat := range stats {
		if stat.ApplicationName == slotName {
			return stat, nil
		}
	}

	return nil, nil
}

// A healthy database replica is:
// 1. Online and accepting connections.
// 2. Is actively replicating from the upstream DB.
// 3. Has a lag of <= 1 second.
func (hc *HealthChecker) CheckReplicationSlot(slotName string) error {
	statReplication, err := hc.getStatReplicationByName(slotName)
	if err != nil {
		return err
	}

	// NOTE: It would be nice to differentiate between not found and inactive.
	// But, is it worth an extra query?
	if statReplication == nil {
		return ErrReplicationSlotNotFound
	}

	if statReplication.LagFromUpstream() > time.Second {
		return ErrReplicationSlotLagTooHigh
	}

	// The DB is healthy.
	return nil
}
