package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func newConn() *sql.DB {
	username := ""
	password := ""
	host := ""
	port := -1
	database := ""

	// build the DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	// Open the connection
	_db, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(err)
	}

	return _db
}

type conn struct {
	db *sql.DB
}

type cpool struct {
	mu      *sync.Mutex
	channel chan interface{}
	conns   []*conn
	maxConn int
}

func NewCPool(maxConn int) (*cpool, error) {
	var mu = sync.Mutex{}
	pool := &cpool{
		mu:      &mu,
		conns:   make([]*conn, 0, maxConn),
		maxConn: maxConn,
		channel: make(chan interface{}, maxConn),
	}

	for i := 0; i < maxConn; i++ {
		pool.conns = append(pool.conns, &conn{newConn()})
		pool.channel <- nil
	}

	return pool, nil
}

func (pool *cpool) Close() {
	close(pool.channel)
	for i := range pool.conns {
		pool.conns[i].db.Close()
	}
}

func (pool *cpool) Get() (*conn, error) {
	<-pool.channel

	pool.mu.Lock()
	c := pool.conns[0]
	pool.conns = pool.conns[1:]
	pool.mu.Unlock()

	return c, nil
}

func (pool *cpool) Put(c *conn) {
	pool.mu.Lock()
	pool.conns = append(pool.conns, c)
	pool.mu.Unlock()

	pool.channel <- nil
}

func benchmarkNonPool() {
	startTime := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 85; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			db := newConn()
			_, err := db.Exec("SELECT SLEEP(0.01);")
			if err != nil {
				panic(err)
			}

			db.Close()
		}()
	}

	wg.Wait()
	fmt.Println("Benchmark Non Connection Pool", time.Since(startTime))
}

func benchmarkPool() {
	startTime := time.Now()
	pool, err := NewCPool(10)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := pool.Get()
			if err != nil {
				panic(err)
			}

			_, err = conn.db.Exec("SELECT SLEEP(0.01);")
			if err != nil {
				panic(err)
			}

			pool.Put(conn)
		}()
	}

	wg.Wait()
	fmt.Println("Benchmark Connection Pool", time.Since(startTime))
	pool.Close()
}

func main() {
	// The DB that this is connecting is Azure SQL DB. And the max connections set on it was 85.
	// Although I could've bumped up this number, I did not for this scenario.
	// benchmarkNonPool()
	// 10 -> 84.467459ms
	// 50 -> 1.423140875s
	// 85 -> 477.535917ms
	// 100 -> Errors out saying too many connections
	// 500 -> Errors out saying too many connections

	benchmarkPool()
	// 10 -> 1.187041167s
	// 50 -> 182.242834ms
	// 85 -> 295.966375ms
	// 100 -> 312.034459ms
	// 500 -> 1.371155292s
	// 5000 -> 12.916328292s
}

// Reference
// https://johannes-weigend.medium.com/concurrency-in-go-32a8b1e35337
