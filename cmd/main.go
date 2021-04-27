package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/pashapdev/locker/pkg/locker"

	_ "github.com/lib/pq"
)

func emulateLock(ctx context.Context, pgLocker locker.Locker, key1, key2 int, label string) {
	var l locker.Locker
	var err error
	for {
		l, err = pgLocker.Lock(ctx, 1, 1)
		if err == nil {
			break
		}

		fmt.Printf("%s: %v\n", label, err)
		time.Sleep(time.Second * 2)
	}
	fmt.Printf("%s: got lock (%d,%d)\n", label, key1, key2)

	// long operation
	time.Sleep(time.Second * 10)

	err = l.UnLock(ctx, 1, 1)
	if err != nil {
		fmt.Printf("%s: Can't unlock: %v\n", label, err)
		return
	}
	fmt.Printf("%s: unlock (%d,%d)\n", label, key1, key2)
}

func main() {
	connString := "postgres://postgres:postgres@localhost:5432/postgres"
	db, err := sql.Open("postgres", connString)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		panic(err)
	}
	pgLocker := locker.New(db)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		emulateLock(ctx, pgLocker, 1, 1, "fn1")
		wg.Done()
	}()

	go func() {
		emulateLock(ctx, pgLocker, 1, 1, "fn2")
		wg.Done()
	}()

	wg.Wait()
}
