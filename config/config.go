package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
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
