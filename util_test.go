package simpledb_test

import (
	"log"

	"github.com/garyburd/redigo/redis"
	"github.com/northbright/simpledb"
)

func ExampleGetRedisHashMaxZiplistEntries() {
	var err error
	var redisHashMaxZiplistEntries uint64 = 0

	log.Printf("\n")
	log.Printf("--------- GetRedisHashMaxZiplistEntries() Test Begin --------\n")

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	redisHashMaxZiplistEntries, err = simpledb.GetRedisHashMaxZiplistEntries(c)
	if err != nil {
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
