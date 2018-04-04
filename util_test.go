package simpledb_test

import (
	"log"

	"github.com/gomodule/redigo/redis"
	"github.com/northbright/simpledb"
)

func ExampleGetRedisHashMaxZiplistEntries() {
	var err error
	var c redis.Conn
	var redisHashMaxZiplistEntries uint64 = 0

	log.Printf("\n")
	log.Printf("--------- GetRedisHashMaxZiplistEntries() Test Begin --------\n")

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	if redisHashMaxZiplistEntries, err = simpledb.GetRedisHashMaxZiplistEntries(c); err != nil {
		goto end
	}

	log.Printf("Redis hash-max-ziplist-entries: %v\n", redisHashMaxZiplistEntries)
end:
	if err != nil {
		log.Printf("error: %v\n", err)
	}

	log.Printf("--------- GetRedisHashMaxZiplistEntries() Test End --------\n")
	// Output:
}

func ExampleGetRedisConn() {
	var err error
	var c redis.Conn

	log.Printf("\n")
	log.Printf("--------- GetRedisConn() Test Begin --------\n")

	if c, err = simpledb.GetRedisConn(":6379", ""); err != nil {
		goto end
	}
	defer c.Close()

	log.Printf("OK.\n")
end:
	if err != nil {
		log.Printf("error: %v\n", err)
	}

	log.Printf("--------- GetRedisConn() Test End --------\n")
	// Output:
}
