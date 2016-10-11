package simpledb

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/northbright/redishelper"
)

func GetRedisHashMaxZiplistEntries(c redis.Conn) (redisHashMaxZiplistEntries uint64, err error) {
	config, err := redishelper.GetConfig(c)
	if err != nil {
		goto end
	}

	redisHashMaxZiplistEntries, err = strconv.ParseUint(config["hash-max-ziplist-entries"], 10, 64)
	if err != nil {
		goto end
	}

end:
	if err != nil {
		DebugPrintf("GetRedisHashMaxZiplistEntries() error: %v\n", err)
		return 0, err
	}

	return redisHashMaxZiplistEntries, nil
}
