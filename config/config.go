package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Host             string `yaml:"host"`
	Database         string `yaml:"database"`
	User             string `yaml:"user"`
	Sslmode          string `yaml:"sslmode"`
	BinaryParameters string `yaml:"binary_parameters"`
	Port             string `yaml:"port"`
	Password         string `yaml:"password"`
	MaxHop           int64  `yaml:"max_hop"`
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
