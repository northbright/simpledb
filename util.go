package simpledb

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/northbright/redishelper"
)

// GetRedisHashMaxZiplistEntries gets the Redis "hash-max-ziplist-entries" config value.
func GetRedisHashMaxZiplistEntries(c redis.Conn) (redisHashMaxZiplistEntries uint64, err error) {
	config := map[string]string{}
	if config, err = redishelper.GetConfig(c); err != nil {
		goto end
	}

	if redisHashMaxZiplistEntries, err = strconv.ParseUint(config["hash-max-ziplist-entries"], 10, 64); err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("GetRedisHashMaxZiplistEntries() error: %v\n", err)
		return 0, err
	}

	return redisHashMaxZiplistEntries, nil
}
