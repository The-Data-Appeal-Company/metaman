package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func FromYaml(file string) (Conf, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return Conf{}, err
	}

	var conf Conf
	if err := yaml.Unmarshal(data, &conf); err != nil {
		return Conf{}, err
	}

	return conf, nil
}
