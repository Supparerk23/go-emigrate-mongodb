package cmd

import (
	"fmt"
	"os"
	"strconv"
	db "go-emigrate-mongodb/db"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/bson"
)

// statusCmd represents the status command
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Shows all databases and counts of all the records accross collections",
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
		
		data := [][]string{}

		dbnames, err := src.Databases()
		if err != nil {
			fmt.Errorf("Error: %s", err)
		}

		for _, dbname := range dbnames {

			collnames, err := src.Collections(dbname)

			if err != nil {
				fmt.Errorf("Error: %s", err)
				continue;
			}

			row := []string{dbname,"", "", "", ""}
			data = append(data, row)

			for _, collname := range collnames {
			

				var (
					total    int
					srcTotal int
					dstTotal int
				)
				
				srcLastRecord, err := src.LastRecord(dbname,collname)
				if err != nil {
					continue
				}

				if (srcLastRecord.ID == primitive.ObjectID{}) {
					continue
				}


				filter := bson.M{"_id": bson.M{"$lt": srcLastRecord.ID}}

				total, err = src.CountRecord(dbname,collname,filter)
				if err != nil {
					continue
				}

				srcTotal += total
		
				total, err = dst.CountRecord(dbname,collname,filter)
				if err != nil {
					continue
				}

				dstTotal += total

				rowCol := []string{"", collname, strconv.Itoa(srcTotal), strconv.Itoa(dstTotal), strconv.Itoa(srcTotal - dstTotal)}
				data = append(data, rowCol)
			}

		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"DB","Collection", "Source", "Destination", "Diff"})

		for _, v := range data {
			table.Append(v)
		}

		table.Render()
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)
}
