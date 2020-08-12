package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	Host             string `yaml:"host"`
	Database         string `yaml:"database"`
	User             string `yaml:"user"`
	Sslmode          string `yaml:"sslmode"`
	BinaryParameters string `yaml:"binary_parameters"`
	Port             string `yaml:"port"`
}

func ParseConfig(path string) (*Config, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := new(Config)
	err = yaml.Unmarshal(bytes, c)
	if err != nil {
		return nil, err
	}
	return c, err
}

func (c *Config) Conninfo(path string) (conninfo string) {
	cfg, err := ParseConfig(path)
	if err != nil {
		log.Fatalln("Error parsing config:", err)
	}
	s := "host=%s port=%s database=%s user=%s sslmode=%s binary_parameters=%s"
	conninfo = fmt.Sprintf(s, cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Sslmode, cfg.BinaryParameters)
	return
}
