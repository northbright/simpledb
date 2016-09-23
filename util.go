package simpledb

import (
	//	"fmt"
	//	"strconv"
	"regexp"

	"github.com/garyburd/redigo/redis"
)

func GetRedisConfigFile(c redis.Conn) (configFile string) {
	var err error
	var info string
	pattern := `config_file:(.*)\n`

	info, err = redis.String(c.Do("INFO", "Server"))
	if err != nil {
		return ""
	}

	re := regexp.MustCompile(pattern)
	matched := re.FindStringSubmatch(info)
	if len(matched) != 2 {
		return ""
	}

	return matched[1]
}

func ComputeBucketId(id uint64) uint64 {
	if HashMaxZiplistEntries == 0 {
		return 1
	}
	return id/HashMaxZiplistEntries + 1
}
