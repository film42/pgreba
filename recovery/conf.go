package conf

import (
	"errors"
	"gopkg.in/ini.v1"
)

var (
	ErrPrimaryConninfoMissing = errors.New("err: primary_conninfo missing in conf")
)

type Conf struct {
	file *ini.File
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
