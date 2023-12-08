package db

import (
	"context"
	"time"
	"log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// "go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"fmt"
	"strings"
	"github.com/spf13/viper"
)

var maxBatchSizeDefault int = 100000

var mongoCtxTimeOut = 60

type Connection struct {
	Config    Config
	Client *mongo.Client
	// Session mongo.Session
	// OplogChan chan bson.M
	// Mutex     sync.Mutex
	// Optime    bson.MongoTimestamp
	// NOplog    uint64
	// NDone     uint64
	ContextTimeOut time.Duration
}

type ApplyOpsResponse struct {
	Ok     bool   `bson:"ok"`
	ErrMsg string `bson:"errmsg"`
}

type Oplog struct {
	Timestamp primitive.Timestamp `bson:"ts"`
	HistoryID int64               `bson:"h"`
	Version   int                 `bson:"v"`
	Operation string              `bson:"op"`
	Namespace string              `bson:"ns"`
	Object    bson.D              `bson:"o"`
	Query     bson.D              `bson:"o2"`
}


type LastRecord struct {
	ID primitive.ObjectID `bson:"_id"`
}

func NewConnection(config Config) (*Connection, error){

	c := new(Connection)
	c.Config = config
	c.ContextTimeOut = time.Duration(mongoCtxTimeOut)

	// rp := readpref.Primary()

	// cliWC := &writeconcern.WriteConcern{
	//     W: 2,
	//     Journal: &journal,
	// }

	var err error
	clientOptions := options.Client().ApplyURI(config.URI)
	// .SetReadPreference(readpref.PrimaryPreferred())
	// .SetWriteConcern(cliWC)
	clientOptions = clientOptions.SetMaxPoolSize(100)
	clientOptions = clientOptions.SetMaxConnIdleTime(60 * time.Second)

	mongoClient, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Panicf("Error creating mongo client: %v", err)
		return nil, err
	}

	c.Client = mongoClient

	mongoCtx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()
	if err = mongoClient.Connect(mongoCtx); err != nil {
		log.Panicf("Error connecting to Mongo db: %v", err)
		return nil, err
	}

	if err = mongoClient.Ping(mongoCtx, nil); err != nil {
        log.Panicf("Error ping to Mongo db: %v", err)
        return nil, err
    }

    // var session mongo.Session

	// if session, err = mongoClient.StartSession(); err != nil {
	// 	log.Panicf("Error start session: %v", err)
	// }

	// if err = session.StartTransaction(); err != nil {
	//     log.Panicf("Error start transaction",err)
	// }

	// c.Session = session

    log.Println(fmt.Sprintf("[[INFO] Mongo %s init done",config.DataCenterName))
 
    return c, err
}

func (c *Connection) CloseMongo() {
	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()

	// c.Session.EndSession(ctx)
	// log.Println( fmt.Sprintf("[INFO] Mongo %s close session",c.Config.DataCenterName) )

	c.Client.Disconnect(ctx)
	log.Println( fmt.Sprintf("[INFO] Mongo %s close connection",c.Config.DataCenterName) )
}

func (c *Connection) Databases() ([]string, error) {
	
	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()

	result, err := c.Client.ListDatabaseNames(ctx,bson.D{})

	if err != nil {
		log.Panicf("Error ListDatabaseNames: %v", err)
	}

	var dbnames []string

	sensitiveList := strings.Join(c.Config.sensitiveDb()[:], ",")

	for _, db := range result {

		if c.Config.Database != "" {
			if db == c.Config.Database {
				dbnames = append(dbnames, db)
			}
		}else{
			if !strings.Contains(sensitiveList, db) {
				dbnames = append(dbnames, db)
			}
		}

	}

	return dbnames, nil

}

func (c *Connection) Collections(dbname string) ([]string, error) {

	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()

	collnames, err := c.Client.Database(dbname).ListCollectionNames(ctx,bson.D{})
			
	if err != nil {
		log.Panicf("Error ListCollectionNames: %v", err)
		return []string{}, err
	}

	return collnames, nil
}

func (c *Connection) LastRecord(dbname string, collection_name string) (LastRecord, error) {

	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()

	coll := c.Client.Database(dbname).Collection(collection_name)

	var lastRecord LastRecord

	opts := options.FindOne().SetSort(bson.D{{"$natural", -1}})

	err := coll.FindOne(
		ctx,
		bson.D{},
		opts,
	).Decode(&lastRecord)

	if err != nil {

		if err == mongo.ErrNoDocuments {
			return lastRecord, nil
		}

		log.Panicf("Error LastRecord: %v", err)
		return lastRecord, err
	}

	return lastRecord, nil

}

func (c *Connection) CountRecord(dbname string, collection_name string, filter interface{}) (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()

	coll := c.Client.Database(dbname).Collection(collection_name)

	count, err := coll.CountDocuments(ctx, filter)
	if err != nil {
	    log.Panicf("Error CountRecord: %v", err)
	}

	return int(count), nil

}

func (c *Connection) DatabaseRegExs() ([]primitive.Regex, error) {

	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()

	dbnames, _ := c.Client.ListDatabaseNames(ctx,bson.D{})

	var slice []primitive.Regex

	for _, dbname := range dbnames {
		if dbname == c.Config.Database {
			slice = append(slice, primitive.Regex{Pattern: dbname + ".*", Options: ""})
			// slice = append(slice, primitive.Regex{Pattern: dbname + ".*"})
		}
	}
	return slice, nil
}

func (c *Connection) SyncOplog(dst *Connection) error {

	var (
		restore_query bson.M
		tail_query    bson.M
		oplogEntry    Oplog
		iter          *mongo.Cursor
		sec           primitive.Timestamp
		// ord           primitive.Timestamp
		// err           error
	)

	oplog := c.Client.Database("local").Collection("oplog.rs")

	var head_result struct {
		Timestamp primitive.Timestamp `bson:"ts"`
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
	defer cancel()

	opts := options.FindOne().SetSort(bson.D{{"$natural", -1}})

	err := oplog.FindOne(
		ctx,
		bson.D{},
		opts,
	).Decode(&head_result)

	if err!= nil {
		return err
	}

	restore_query = bson.M{
		"ts": bson.M{"$gt": time.Now().Unix()},
	}

	tail_query = bson.M{
		"ts": bson.M{"$gt": head_result.Timestamp},
	}

	if viper.GetInt("since") > 0 {
		var sec64 int64
		sec64 = int64(viper.GetInt("since"))
		sec = primitive.Timestamp{T: uint32(sec64), I: 0}
		restore_query["ts"] = bson.M{"$gt": sec}
	}

	dbnames, _ := c.DatabaseRegExs()

	if len(dbnames) > 0 {
		restore_query["ns"] = bson.M{"$in": dbnames}
		tail_query["ns"] = bson.M{"$in": dbnames}
	} else {
		return fmt.Errorf("No databases found")
	}


	applyOpsResponse := ApplyOpsResponse{}
	opCount := 0

	if viper.GetInt("since") > 0 {
		fmt.Println("Restoring oplog...")

		iter, err = oplog.Find(ctx, restore_query)

		if err != nil {
			fmt.Println("oplog.Find err",err)
		}


		for iter.Next(ctx) {

			if err := iter.Decode(&oplogEntry); err != nil {
				fmt.Println("iter.Decode err",err)
				continue
			}

			tail_query = bson.M{
				"ts": bson.M{"$gte": oplogEntry.Timestamp},
			}

			// skip noops
			if oplogEntry.Operation == "n" {
				log.Printf("skipping no-op for namespace `%v`", oplogEntry.Namespace)
				continue
			}
			opCount++

			// apply the operation
			opsToApply := []Oplog{oplogEntry}
			//dothis
			
			// if oplogEntry.Operation == "u"{
			// 	// fmt.Println(oplogEntry)
			// 	// fmt.Println(oplogEntry.Query[0].Key,oplogEntry.Query[0].Value)

			// 	// if oplogEntry.Query[0].Key == "_id" {
			// 	// 	oplogEntry.Object[0].Key = oplogEntry.Query[0].Key
			// 	// 	oplogEntry.Object[0].Value = oplogEntry.Query[0].Value
			// 	// }

			// 	// if oplogEntry.Object[0].Key == "$v" {
			// 	// 	oplogEntry.Object[0].Value = 2

			// 	// 	tmp := oplogEntry.Object[1]

			// 	// 	oplogEntry.Object[1].Key = "diff"
			// 	// 	oplogEntry.Object[1].Value = tmp
			// 	// }

			// }


			opts := options.RunCmd().SetReadPreference(readpref.Primary())

			err := dst.Client.Database(dst.Config.Database).RunCommand(ctx, bson.M{"applyOps": opsToApply}, opts).Decode(&applyOpsResponse)
			if err != nil {
				
				if strings.Contains(err.Error(),"Expected _id") {
					//skip update doc but not have doc error
					applyOpsResponse.Ok = true
					fmt.Println(err)
				}else{
					return err
				}
			}

			// check the server's response for an issue
			if !applyOpsResponse.Ok {
				return fmt.Errorf("server gave error applying ops: %v", applyOpsResponse.ErrMsg)
			}

			fmt.Println(opCount,oplogEntry.Namespace,oplogEntry.Operation,oplogEntry.Timestamp)
		}
	}

	fmt.Println("Tailing.....")
	// 1 * time.Second
	optsTail := options.Find().SetMaxAwaitTime(1 * time.Second)

	iter, err = oplog.Find(
		ctx,
		tail_query,
		optsTail,
	)

	for {

		ctxForever, cancelForever := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
		defer cancelForever()

		for iter.Next(context.Background()) {

			if err := iter.Decode(&oplogEntry); err != nil {
				fmt.Println("iter.Decode err",err)
				continue
			}

			if oplogEntry.Operation == "n" {
				// log.Printf("skipping no-op for namespace `%v`", oplogEntry.Namespace)
				continue
			}

			if !strings.Contains(oplogEntry.Namespace, c.Config.Database+".") {
				// log.Printf("skipping namespace `%v`", oplogEntry.Namespace)
				continue
			}

			
			collection := strings.Split(oplogEntry.Namespace, ".")[1]

			if len(c.Config.Collections) > 0 {

				// check collection against config

				isCollectionMatch := false
				for _, permittedCollection := range c.Config.Collections {
					if collection == permittedCollection {
						isCollectionMatch = true
					}
				}

				if !isCollectionMatch {
					log.Printf("skipping collection `%v`", oplogEntry.Namespace)
					continue
				}

			}

			oplogEntry.Namespace = dst.Config.Database + "." + collection

			if false {
				fmt.Println("\n")
				fmt.Println("****************************** %v", oplogEntry.HistoryID)
				fmt.Println("****************************** %v", oplogEntry.Namespace)
				fmt.Println("****************************** %v", oplogEntry.Object)
				fmt.Println("****************************** %v", oplogEntry.Operation)
				fmt.Println("****************************** %v", oplogEntry.Query)
				fmt.Println("****************************** %v", oplogEntry.Timestamp)
				fmt.Println("****************************** %v", oplogEntry.Version)
				fmt.Println("%v", oplogEntry.Namespace)
			}

			// apply the operation
			opCount++
			opsToApply := []Oplog{oplogEntry}

			opts := options.RunCmd().SetReadPreference(readpref.Primary())

			err := dst.Client.Database(dst.Config.Database).RunCommand(ctxForever, bson.M{"applyOps": opsToApply}, opts).Decode(&applyOpsResponse)
			if err != nil {
				
				if strings.Contains(err.Error(),"Expected _id") {
					//skip update doc but not have doc error
					applyOpsResponse.Ok = true
					fmt.Println(err)
				}else{
					return err
				}
			}

			// check the server's response for an issue
			if !applyOpsResponse.Ok {
				return fmt.Errorf("server gave error applying ops: %v", applyOpsResponse.ErrMsg)
			}

			fmt.Println(opCount,oplogEntry.Namespace,oplogEntry.Operation,oplogEntry.Timestamp)

		}

		if err := iter.Err(); err != nil {
			fmt.Println("ter.Err",err)
		}

		if err := iter.Close(ctxForever); err != nil {
			fmt.Println("iter.Close",err)
		}


		tail_query = bson.M{
			"ts": bson.M{"$gte": oplogEntry.Timestamp},
		}

		time.Sleep(5 * time.Second) 

		iter, err = oplog.Find(
			ctxForever,
			tail_query,
			optsTail,
		)

	}

	return nil

}