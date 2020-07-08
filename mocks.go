package main

import (
	"time"
)

type fakeDataSource struct{}

func (fdr *fakeDataSource) Close() error {
	return nil
}

func (fdr *fakeDataSource) GetPgStatReplication() ([]*PgStatReplication, error) {
	return []*PgStatReplication{
		{
			ApplicationName: "pghost_created_replication_slot",
			FlushLag:        time.Second,
		},
	}, nil
}
func (fdr *fakeDataSource) GetPgReplicationSlots() ([]*PgReplicationSlot, error) {
	return []*PgReplicationSlot{
		{
			SlotName: "pghost_created_replication_slot",
			Active:   true,
		},
	}, nil
}
