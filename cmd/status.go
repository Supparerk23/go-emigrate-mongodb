package cmd

import (
	"fmt"
	"os"
	"strconv"

	// db "github.com/checkr/go-sync-mongo/db"
	db "go-sync-mongo/db"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
)

type LastRecord struct {
	ID bson.ObjectId `bson:"_id"`
}

// statusCmd represents the status command
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Shows all databases and counts of all the records accross collections",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		srcUrl := viper.GetString("src")
		if srcUrl == "" {
			srcUrl = os.Getenv("SOURCE_MONGO_URI")
		}

		srcUsername := viper.GetString("src-username")
		if srcUsername == "" {
			srcUsername = os.Getenv("SOURCE_MONGO_USERNAME")
		}

		srcPassword := viper.GetString("src-password")
		if srcPassword == "" {
			srcPassword = os.Getenv("SOURCE_MONGO_PASSWORD")
		}

		srcConfig := db.Config{
			URI: srcUrl,
			SSL: viper.GetBool("src-ssl"),
			Creds: mgo.Credential{
				Username: srcUsername,
				Password: srcPassword,
			},
		}
		fmt.Println("srcConfig",srcConfig)
		src, err := db.NewConnection(srcConfig)
		if err != nil {
			log.Panic(err)
		}

		dstUrl := viper.GetString("dst")
		if dstUrl == "" {
			dstUrl = os.Getenv("DESTINATION_MONGO_URI")
		}

		dstUsername := viper.GetString("dst-username")
		if dstUsername == "" {
			dstUsername = os.Getenv("DESTINATION_MONGO_USERNAME")
		}

		dstPassword := viper.GetString("dst-password")
		if dstPassword == "" {
			dstPassword = os.Getenv("DESTINATION_MONGO_PASSWORD")
		}

		dstConfig := db.Config{
			URI: dstUrl,
			SSL: viper.GetBool("dst-ssl"),
			Creds: mgo.Credential{
				Username: dstUsername,
				Password: dstPassword,
			},
		}
		fmt.Println("dstConfig",dstConfig)
		dst, err := db.NewConnection(dstConfig)
		if err != nil {
			log.Panic(err)
		}

		data := [][]string{}

		dbnames, err := src.Databases()
		if err != nil {
			fmt.Errorf("Error: %s", err)
		}
		fmt.Println("src dbnames",dbnames)
		dstdbnames, _ := dst.Databases()
		fmt.Println("dst dbnames",dstdbnames)

		for _, dbname := range dbnames {

			collnames, err := src.Session.DB(dbname).CollectionNames()
			if err != nil {
				fmt.Errorf("Error: %s", err)
			}

			row := []string{dbname,"", "", "", ""}
			data = append(data, row)

			for _, collname := range collnames {

				var (
					total    int
					srcTotal int
					dstTotal int
				)

				srcColl := src.Session.DB(dbname).C(collname)
				var srcLastRecord LastRecord
				_ = srcColl.Find(nil).Sort("-$natural").Limit(1).One(&srcLastRecord)
				srcQuery := srcColl.Find(bson.M{"_id": bson.M{"$lt": srcLastRecord.ID}})
				total, _ = srcQuery.Count()
				srcTotal += total
				fmt.Println("dbname",dbname,"collname",collname)
				dstColl := dst.Session.DB(dbname).C(collname)
				fmt.Println("dstColl",dstColl)
				dstQuery := dstColl.Find(bson.M{"_id": bson.M{"$lt": srcLastRecord.ID}})
				total, _ = dstQuery.Count()
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
