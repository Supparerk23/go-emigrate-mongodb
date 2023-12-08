package cmd

import (
	"fmt"
	"log"
	db "go-emigrate-mongodb/db"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Tails the source oplog and syncs to destination",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		
		srcConfig := db.SourceConnfLoad()

		src, err := db.NewConnection(srcConfig)
		defer src.CloseMongo()
		if err != nil {
			log.Panic(err)
		}

		dstConfig := db.DestinationConfLoad()
		
		dst, err := db.NewConnection(dstConfig)
		defer dst.CloseMongo()
		if err != nil {
			log.Panic(err)
		}

		err = src.SyncOplog(dst)
		if err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(syncCmd)
	syncCmd.Flags().Int32("since", 0, "seconds since the Unix epoch")
	syncCmd.Flags().Int32("ordinal", 0, "incrementing ordinal for operations within a given second")
	viper.BindPFlag("since", syncCmd.Flags().Lookup("since"))
	viper.BindPFlag("ordinal", syncCmd.Flags().Lookup("ordinal"))
}
