package conf

import (
	"testing"
)

func TestCanLoadRecoveryConf(t *testing.T) {
	conf := `
################################################################################
# Managed by Ansible
################################################################################

#------------------------------------------------------------------------------
# STANDBY SERVER SETTINGS
#------------------------------------------------------------------------------
standby_mode      = 'on'
primary_conninfo  = 'host=upstream-db1 port=5432 user=replicator password=hunter2 application_name=some-standby-db1 sslmode=verify-ca sslrootcert=/etc/postgresql/10/some_pg_dir/sslrootcert'
primary_slot_name = 'some-standby-db1'
recovery_target_timeline = 'latest'
`
	c, err := Parse([]byte(conf))
	if err != nil {
		t.Fatal(err)
	}

	expected := "host=upstream-db1 port=5432 user=replicator password=hunter2 application_name=some-standby-db1 sslmode=verify-ca sslrootcert=/etc/postgresql/10/some_pg_dir/sslrootcert"

	conninfo, _ := c.GetPrimaryConninfo()
	if conninfo != expected {
		t.Fatal("Conninfo did not match the expected value. Found:", conninfo)
	}
}

func TestMissingPrimaryConninfoError(t *testing.T) {
	conf := `
standby_mode      = 'on'
`
	c, err := Parse([]byte(conf))
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.GetPrimaryConninfo()
	if err != ErrPrimaryConninfoMissing {
		t.Fatal("expected a primary conninfo missing err but found this err instead:", err)
	}
}
