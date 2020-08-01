package conf

import (
	"errors"
	"github.com/jmoiron/sqlx"
	"gopkg.in/ini.v1"
)

var (
	ErrMissingRecoveryConf    = errors.New("err: missing recovery conf")
	ErrPrimaryConninfoMissing = errors.New("err: primary_conninfo missing in conf")
)

type Conf struct {
	file *ini.File
}

func FetchAndParseRecoveryConfFromDB(db *sqlx.DB) (*Conf, error) {
	// Attempt to load recovery.conf from disk.
	sql := `select * from pg_read_file('recovery.conf')`
	rows, err := db.Queryx(sql)
	if err != nil {
		// TODO: I think this might return an error when the file is missing.
		return nil, err
	}
	defer rows.Close()

	// If the file does not exist, we'll recieve an empty result.
	if !rows.Next() {
		return nil, ErrMissingRecoveryConf
	}

	// Otherwise, we can pull the file bytes from the first and only column.
	var recoveryConf string
	err = rows.Scan(&recoveryConf)
	if err != nil {
		return nil, err
	}

	// And then we can cast and feed into the Parse func.
	return Parse([]byte(recoveryConf))
}

func Parse(conf []byte) (*Conf, error) {
	file, err := ini.Load(conf)
	if err != nil {
		return nil, err
	}
	return &Conf{file}, nil
}

func (c *Conf) GetPrimaryConninfo() (string, error) {
	conninfo := c.file.Section("").Key("primary_conninfo").String()
	if len(conninfo) == 0 {
		return "", ErrPrimaryConninfoMissing
	}
	return conninfo, nil
}
