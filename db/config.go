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

	// srcUsername := viper.GetString("src-username")
	// if srcUsername == "" {
	// 	srcUsername = os.Getenv("SOURCE_MONGO_USERNAME")
	// }

	// srcPassword := viper.GetString("src-password")
	// if srcPassword == "" {
	// 	srcPassword = os.Getenv("SOURCE_MONGO_PASSWORD")
	// }

	return Config{
		DataCenterName : "Source",
		URI: srcUrl,
		SSL: viper.GetBool("src-ssl"),
		Database: viper.GetString("src-db"),
		Collections: strings.Split(viper.GetString("src-collections"), ","),
	}
}

func DestinationConfLoad() Config {

	dstUrl := viper.GetString("dst")
	if dstUrl == "" {
		dstUrl = os.Getenv("DESTINATION_MONGO_URI")
	}

	// dstUsername := viper.GetString("dst-username")
	// if dstUsername == "" {
	// 	dstUsername = os.Getenv("DESTINATION_MONGO_USERNAME")
	// }

	// dstPassword := viper.GetString("dst-password")
	// if dstPassword == "" {
	// 	dstPassword = os.Getenv("DESTINATION_MONGO_PASSWORD")
	// }

	return Config{
		DataCenterName : "Destination",
		URI: dstUrl,
		SSL: viper.GetBool("dst-ssl"),
		Database:    viper.GetString("dst-db"),
		Collections: make([]string, 0),
	}
}
