package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/subosito/gotenv"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "go-sync-mongo",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	gotenv.Load()
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports Persistent Flags, which, if defined here,
	// will be global for your application.

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.go-sync-mongo.yaml)")
	RootCmd.PersistentFlags().String("src", "", "mongodb://host1:27017 ,optional can use in env")
	RootCmd.PersistentFlags().String("src-db", "", "source database name")
	RootCmd.PersistentFlags().String("src-collections", "", "source collections separated by ,")
	RootCmd.PersistentFlags().String("src-username", "", "source database username ,optional can use in env")
	RootCmd.PersistentFlags().String("src-password", "", "source database password ,optional can use in env")
	RootCmd.PersistentFlags().Bool("src-ssl", false, "source ssl enabled (true)")

	RootCmd.PersistentFlags().String("dst", "", "mongodb://host1:27017,host2:27017 ,optional can use in env")
	RootCmd.PersistentFlags().String("dst-db", "", "target database name")
	RootCmd.PersistentFlags().String("dst-collections", "", "dst collections separated by ,")
	RootCmd.PersistentFlags().String("dst-username", "", "destination database username ,optional can use in env")
	RootCmd.PersistentFlags().String("dst-password", "", "destiantion database password ,optional can use in env")
	RootCmd.PersistentFlags().Bool("dst-ssl", false, "destination ssl enabled (true)")

	viper.BindPFlag("src", RootCmd.PersistentFlags().Lookup("src"))
	viper.BindPFlag("src-db", RootCmd.PersistentFlags().Lookup("src-db"))
	viper.BindPFlag("src-collections", RootCmd.PersistentFlags().Lookup("src-collections"))
	viper.BindPFlag("src-username", RootCmd.PersistentFlags().Lookup("src-username"))
	viper.BindPFlag("src-password", RootCmd.PersistentFlags().Lookup("src-password"))
	viper.BindPFlag("src-ssl", RootCmd.PersistentFlags().Lookup("src-ssl"))
	viper.BindPFlag("dst", RootCmd.PersistentFlags().Lookup("dst"))
	viper.BindPFlag("dst-db", RootCmd.PersistentFlags().Lookup("dst-db"))
	viper.BindPFlag("dst-username", RootCmd.PersistentFlags().Lookup("dst-username"))
	viper.BindPFlag("dst-password", RootCmd.PersistentFlags().Lookup("dst-password"))
	viper.BindPFlag("dst-ssl", RootCmd.PersistentFlags().Lookup("dst-ssl"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	}
	viper.SetConfigName(".go-sync-mongo") // name of config file (without extension)
	viper.AddConfigPath("$HOME")          // adding home directory as first search path
	viper.AddConfigPath(".")
	viper.AutomaticEnv() // read in environment variables that match
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("GSM")

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
