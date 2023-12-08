package db

import (
	"os"
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	DataCenterName string
	URI         string
	SSL         bool
	Database    string
	Collections []string
}

// func (p *Config) Load() error {
// 	return nil
// }

// func (p *Config) validate() error {
// 	return nil
// }

func (p *Config) sensitiveDb() []string {
	return []string{"admin","config","local"}
}

func SourceConnfLoad() Config {

	srcUrl := viper.GetString("src")
	if srcUrl == "" {
		srcUrl = os.Getenv("SOURCE_MONGO_URI")
	}

	collections := []string{}

	if viper.GetString("src-collections") != "" {
		collections = strings.Split(viper.GetString("src-collections"), ",")
	}
	

	return Config{
		DataCenterName : "Source",
		URI: srcUrl,
		SSL: viper.GetBool("src-ssl"),
		Database: viper.GetString("src-db"),
		Collections: collections,
	}
}

func DestinationConfLoad() Config {

	dstUrl := viper.GetString("dst")
	if dstUrl == "" {
		dstUrl = os.Getenv("DESTINATION_MONGO_URI")
	}

	return Config{
		DataCenterName : "Destination",
		URI: dstUrl,
		SSL: viper.GetBool("dst-ssl"),
		Database:    viper.GetString("dst-db"),
		Collections: make([]string, 0),
	}
}
