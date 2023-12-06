package db

import (
	"context"
	"time"
	// "os"
	"log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// "go.mongodb.org/mongo-driver/mongo/readconcern"
	// "go.mongodb.org/mongo-driver/mongo/readpref"
	// "strconv"
	"fmt"
	"strings"
)

var maxBatchSizeDefault int = 100000

var mongoCtxTimeOut = 600

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

// func (c *Connection) Find(dbname string, collection_name string, filter interface{}, opts *options.FindOptions) (interface{}, error){

// 	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
// 	defer cancel()

// 	col, err := c.Client.Database(dbname).Collection(collection_name)

// 	if err != nil {
// 		log.Panicf("Error Collection: %v", err)
// 		return interface{}, err
// 	}


// 	cur, err := col.Find(ctx, bson.D{{}}, findOptions)
//     if err !=nil {
//         log.Fatal(err)
//     }


// }

// func (c *Connection) Databases() ([]string, error) {

// 	ctx, cancel := context.WithTimeout(context.Background(), c.ContextTimeOut*time.Second)
// 	defer cancel()

// 	opts := options.Session().SetDefaultReadConcern(readconcern.Majority())
// 	sess, err := c.Client.StartSession(opts)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	defer sess.EndSession(ctx)

// 	txnOpts := options.Transaction().SetReadPreference(readpref.PrimaryPreferred())

// 	result, err := sess.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {

// 			result, err := c.Client.ListDatabaseNames(
// 				ctx,
// 				bson.D{})
		
// 			if err != nil {
// 				fmt.Println("Error ListDatabaseNames")
// 				return nil, err
// 			}

// 			fmt.Println("result in",result)

// 			return result, nil

// 	}, txnOpts)

// 	if err != nil {
// 		log.Panicf("Error mongo sesison: %v", err)
// 	}

// 	fmt.Println("result out",result)

// 	var dbnames []string

// 	// sensitiveList := strings.Join(c.Config.sensitiveDb()[:], ",")

// 	// for _, db := range result {

// 	// 	if c.Config.Database != "" {
// 	// 		if db == c.Config.Database {
// 	// 			dbnames = append(dbnames, db)
// 	// 		}
// 	// 	}else{
// 	// 		if !strings.Contains(sensitiveList, db) {
// 	// 			dbnames = append(dbnames, db)
// 	// 		}
// 	// 	}

// 	// }

// 	return dbnames, nil

// }