package main

import (
	"time"

	"gopkg.in/volatiletech/null.v6"
)

type fakeDataSource struct {
	byteLag int64
}

func (fdr *fakeDataSource) Close() error {
	return nil
}

func (fdr *fakeDataSource) GetNodeInfo() (*NodeInfo, error) {
	return &NodeInfo{
		State:               1,
		PostmasterStartTime: "2020-11-12 10:55:55.073 EST",
		Role:                "primary",
		Xlog: &XlogInfo{
			Location:         137936246584,
			ReceivedLocation: 137936246408,
			ReplayedLocation: null.NewInt64(137936246408, true),
			Paused:           false,
		},
		ByteLag: fdr.byteLag,
	}, nil
}

func (fdr *fakeDataSource) IsInRecovery() (bool, error) {
	return false, nil
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
