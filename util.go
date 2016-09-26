package simpledb

import (
	"errors"
	"io/ioutil"
	"regexp"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

func GetRedisConfigFile(c redis.Conn) (configFile string) {
	var err error
	var info string
	var re *regexp.Regexp
	pattern := `config_file:(.*\.conf)`

	info, err = redis.String(c.Do("INFO", "Server"))
	if err != nil {
		return ""
	}

	re = regexp.MustCompile(pattern)
	matched := re.FindStringSubmatch(info)
	if len(matched) != 2 {
		return ""
	}

	return matched[1]
}

func GetRedisHashMaxZiplistEntries(configFile string) (redisHashMaxZiplistEntries uint64) {
	var err error
	var re *regexp.Regexp
	buf := []byte{}
	pattern := `hash-max-ziplist-entries\s(\d*)`
	matched := []string{}

	if len(configFile) == 0 {
		err = errors.New("Redis config file name is empty.")
		goto end
	}

	buf, err = ioutil.ReadFile(configFile)
	if err != nil {
		goto end
	}

	re = regexp.MustCompile(pattern)
	matched = re.FindStringSubmatch(string(buf))
	if len(matched) != 2 {
		err = errors.New("No hash-nax-ziplist-entries found.")
		goto end
	}

	redisHashMaxZiplistEntries, err = strconv.ParseUint(matched[1], 10, 64)
	if err != nil {
		goto end
	}

end:
	if err != nil {
		DebugPrintf("GetRedisHashMaxZiplistEntries() error: %v. Use default: %v\n", err, DefRedisHashMaxZiplistEntries)
		return DefRedisHashMaxZiplistEntries
	}

	return redisHashMaxZiplistEntries
}

func ComputeBucketId(id uint64) uint64 {
	return DefRedisHashMaxZiplistEntries
}
