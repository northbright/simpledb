package simpledb

import (
	"fmt"
	"strconv"

	"github.com/gomodule/redigo/redis"
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

// GetRedisConn gets the Redis connection.
func GetRedisConn(redisAddr, redisPassword string) (c redis.Conn, err error) {
	pongStr := ""

	if c, err = redis.Dial("tcp", redisAddr); err != nil {
		goto end
	}

	if len(redisPassword) != 0 {
		if _, err = c.Do("AUTH", redisPassword); err != nil {
			goto end
		}
	}

	if pongStr, err = redis.String(c.Do("PING")); err != nil {
		goto end
	}

	if pongStr != "PONG" {
		err = fmt.Errorf("Redis PING != PONG(%v)", pongStr)
		goto end
	}
end:
	if err != nil {
		debugPrintf("GetRedisConn() error: %v\n", err)
		return c, err
	}
	return c, nil
}
