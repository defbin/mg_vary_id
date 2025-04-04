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

	err = source.Database("test").Drop(ctx)
	if err != nil {
		log.Fatalf(`delete "test" database: %s`, err)
	}

	const numIter = 500
	resusable := make(chan []bson.D, runtime.NumCPU()*2)
	docC := make(chan []bson.D, runtime.NumCPU())

	data := make([]byte, 1_000_000)
	rand.Read(data)

	for range cap(resusable) {
		docs := make([]bson.D, 43)
		for i := range len(docs) - 1 {
			docs[i] = bson.D{{"_id", nil}, {"data", data}}
		}
		docs[len(docs)-1] = bson.D{{"data", data}}

		resusable <- docs
	}

	go func() {
		defer close(docC)

		data := make([]byte, 1_000_000)
		rand.Read(data)

		for i := range numIter {
			oid := bson.NewObjectID()
			docs := <-resusable

			for j := 0; j < len(docs)-1; j += 6 {
				docs[j+0][0].Value = bson.NewObjectID()
				docs[j+1][0].Value = strconv.Itoa(i) + ":" + strconv.Itoa(j+1)
				docs[j+2][0].Value = i*100 + j + 2
				docs[j+3][0].Value = bson.Timestamp{uint32(i), uint32(j + 3)}
				docs[j+4][0].Value = struct{ A, B int32 }{int32(i), int32(j + 4)}
				docs[j+5][0].Value = time.Now().Add(time.Duration(i*100+j+5) * time.Second)
			}

			docs[len(docs)-1][0].Value = oid

			docC <- docs
		}
	}()

	var globalInc atomic.Int64
	grp, grpCtx := errgroup.WithContext(ctx)

	for range runtime.NumCPU() {
		grp.Go(func() error {
			coll := source.Database("test").Collection("vary_id")

			for docs := range docC {
				_, err = coll.InsertMany(grpCtx, docs)
				if err != nil {
					return err
				}

				resusable <- docs

				fmt.Printf("%d ", numIter-globalInc.Add(1))
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
