package config

import (
	"fmt"
	"net/url"
)

type Conf struct {
	Metastore  Metastore  `yaml:"metastore"`
	Aws        Aws        `yaml:"aws"`
	Prometheus Prometheus `yaml:"prometheus"`
	Db         Db         `yaml:"db"`
}

type Aws struct {
	Region string `yaml:"region"`
}

type Metastore struct {
	Hive Hive `yaml:"hive"`
}

type Hive struct {
	Url  string `yaml:"url"`
	Port int    `yaml:"port"`
}

type Prometheus struct {
	Enabled bool `yaml:"enabled"`
}

type Db struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"ssl_mode"`
	Driver   string `yaml:"driver"`
}

func (p *Db) ConnectionString() string {
	return fmt.Sprintf("%s://%s:%s@%s:%d/%s?sslmode=%s", p.Driver, p.User, url.QueryEscape(p.Password), p.Host, p.Port, p.Database, p.SSLMode)
}
