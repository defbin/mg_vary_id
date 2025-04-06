package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"golang.org/x/sync/errgroup"
)

const (
	dbName   = "test"
	collName = "vary_id"
)

func main() {
	ctx := context.Background()

	if len(os.Args) < 2 {
		log.Fatal("missing database uri")
	}

	source, err := mongoConnect(ctx, os.Args[1])
	if err != nil {
		log.Fatalf("connect to source: %v", err)
	}

	defer source.Disconnect(context.Background())

	err = source.Database(dbName).Collection(collName).Drop(ctx)
	if err != nil {
		log.Fatalf(`drop %s.%s collection: %s`, dbName, collName, err)
	}

	const numIter = 1000
	resusable := make(chan []bson.D, runtime.NumCPU()*2)
	docC := make(chan []bson.D, runtime.NumCPU())

	data := make([]byte, 1_000)
	rand.Read(data)

	for range cap(resusable) {
		docs := make([]bson.D, 42000)
		for i := range len(docs) {
			docs[i] = bson.D{{"_id", nil}, {"data", data}}
		}

		resusable <- docs
	}

	go func() {
		defer close(docC)

		for i := range numIter {
			docs := <-resusable

			for j := 0; j < len(docs)-1; j += 6 {
				docs[j+0][0].Value = bson.NewObjectID()
				docs[j+1][0].Value = strconv.Itoa(i) + ":" + strconv.Itoa(j+1)
				docs[j+2][0].Value = i*1000000 + j + 2
				docs[j+3][0].Value = bson.Timestamp{uint32(i), uint32(j + 3)}
				docs[j+4][0].Value = struct{ A, B int32 }{int32(i), int32(j + 4)}
				docs[j+5][0].Value = time.Now().Add(time.Duration(i*1000000+j+5) * time.Second)
			}

			docC <- docs
		}
	}()

	var globalInc atomic.Int64
	globalInc.Store(numIter)

	grp, grpCtx := errgroup.WithContext(ctx)

	for range runtime.NumCPU() {
		grp.Go(func() error {
			coll := source.Database(dbName).Collection(collName)

			for docs := range docC {
				_, err = coll.InsertMany(grpCtx, docs)
				if err != nil {
					return err
				}

				resusable <- docs

				fmt.Printf("%d ", globalInc.Add(-1))
			}

			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Done.")
}

func mongoConnect(ctx context.Context, uri string) (*mongo.Client, error) {
	if uri == "" {
		return nil, errors.New("invalid MongoDB URI")
	}

	opts := options.Client().ApplyURI(uri).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.Majority())

	conn, err := mongo.Connect(opts)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		conn.Disconnect(ctx)

		return nil, fmt.Errorf("ping: %w", err)
	}

	return conn, nil
}
