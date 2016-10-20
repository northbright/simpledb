package simpledb_test

import (
	"github.com/garyburd/redigo/redis"
	"github.com/northbright/simpledb"
)

func Example_GetRedisHashMaxZiplistEntries() {
	var err error
	var redisHashMaxZiplistEntries uint64 = 0

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- GetRedisHashMaxZiplistEntries() Test Begin --------\n")

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	redisHashMaxZiplistEntries, err = simpledb.GetRedisHashMaxZiplistEntries(c)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Redis hash-max-ziplist-entries: %v\n", redisHashMaxZiplistEntries)
end:
	if err != nil {
		simpledb.DebugPrintf("error: %v\n", err)
	}

	simpledb.DebugPrintf("--------- GetRedisHashMaxZiplistEntries() Test End --------\n")
	// Output:
}
