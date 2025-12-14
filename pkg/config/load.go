package config

import (
	"github.com/spf13/viper"
)

var cfg Config

// Load unmarshals viper into Config.
func Load() (Config, error) {
	var c Config
	if err := viper.Unmarshal(&c); err != nil {
		return c, err
	}
	cfg = c
	return c, nil
}

// Get returns the cached config after Load.
func Get() Config { return cfg }
