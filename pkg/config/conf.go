package config

type Conf struct {
	Metastore  Metastore  `yaml:"metastore"`
	Aws        Aws        `yaml:"aws"`
	Prometheus Prometheus `yaml:"prometheus"`
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
