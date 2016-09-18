package simpledb

import (
	//"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

type DB struct {
	Name string
}

func Open(name string) *DB {
	return &DB{Name: name}
}

func (db *DB) Close() {
}

func (db *DB) GenMaxIdKey() (maxIdKey string) {
	return fmt.Sprintf("%v/maxid", db.Name)
}

func (db *DB) GetMaxId(c redis.Conn) (maxId uint64, err error) {
	k := db.GenMaxIdKey()
	exists, err := redis.Bool(c.Do("EXISTS", k))
	if err != nil {
		debugPrintf("GenMaxId() error: %v\n", err)
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	maxId, err = redis.Uint64(c.Do("GET", k))
	if err != nil {
		debugPrintf("GenMaxId() error: %v\n", err)
		return 0, err
	}

	return maxId, nil
}

func (db *DB) GenMaxBucketIdKey() (maxBucketIdKey string) {
	return fmt.Sprintf("%v/maxbucketid", db.Name)
}

func (db *DB) GetMaxBucketId(c redis.Conn) (maxBucketId uint64, err error) {
	k := db.GenMaxBucketIdKey()
	exists, err := redis.Bool(c.Do("EXISTS", k))
	if err != nil {
		debugPrintf("GetMaxBucketId() error: %v\n", err)
		return 0, err
	}

	if !exists {
		_, err := redis.String(c.Do("SET", k, 1))
		if err != nil {
			debugPrintf("GetMaxBucketId() error: %v\n", err)
			return 0, err
		}

		return 1, nil
	}

	maxBucketId, err = redis.Uint64(c.Do("GET", k))
	if err != nil {
		debugPrintf("GetMaxBucketId() error: %v\n", err)
		return 0, err
	}

	return maxBucketId, nil
}

func (db *DB) GenHashKey(bucketId uint64) (recordHashKey, indexHashKey string) {
	bucketIdStr := strconv.FormatUint(bucketId, 10)
	recordHashKey = fmt.Sprintf("%v/bucket/%v", db.Name, bucketIdStr)
	indexHashKey = fmt.Sprintf("%v/idx/bucket/%v", db.Name, bucketIdStr)
	return recordHashKey, indexHashKey
}

func (db *DB) FindIndexHashKey(c redis.Conn, jsonData string) (indexHashKey string, err error) {
	maxBucketId, err := db.GetMaxBucketId(c)
	if err != nil {
		debugPrintf("GetMaxBucketId() error: %v\n", err)
		return "", err
	}

	indexHashKey = ""
	indexHashField := jsonData

	for i := maxBucketId; i >= 1; i-- {
		_, indexHashKey = db.GenHashKey(i)
		exists, err := redis.Bool(c.Do("HEXISTS", indexHashKey, indexHashField))
		if err != nil {
			debugPrintf("FindIndexHashKey() error: %v\n", err)
			return "", err
		}

		if exists {
			return indexHashKey, nil
		}
	}
	return "", nil
}

func (db *DB) Create(c redis.Conn, jsonData string) (id uint64, err error) {
	// 1. Check json data.
	if len(jsonData) == 0 {
		err = errors.New("Empty jsonData.")
		debugPrintf("Create() error: %v\n", err)
		return 0, err
	}

	// 2. Get max id and compute current id.
	maxId, err := db.GetMaxId(c)
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return 0, err
	}

	id = maxId + 1

	// 3. Compute current bucket id.
	//    Increase max bucket id if current bucket id > max bucket id.
	bucketId := ComputeBucketId(id)

	maxBucketId, err := db.GetMaxBucketId(c)
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return 0, err
	}

	if bucketId > maxBucketId {
		k := db.GenMaxBucketIdKey()
		_, err := c.Do("INCR", k)
		if err != nil {
			debugPrintf("Create() error: %v\n", err)
			return 0, err
		}
	}

	// 4. Generate hash key for record and index.
	recordHashKey, indexHashKey := db.GenHashKey(bucketId)

	// 5. Check if json data already exists.
	oldIndexHashKey, err := db.FindIndexHashKey(c, jsonData)
	if err != nil {
		debugPrintf("Create(): error: %v\n", err)
		return 0, err
	}

	// Index already exists, it means the record also exists.
	if oldIndexHashKey != "" {
		err = errors.New("JSON data already exists.")
		debugPrintf("Create(): error: %v\n", err)
		return 0, err
	}

	// 6. Create record and index
	recordHashField := id
	indexHashField := jsonData
	maxIdKey := db.GenMaxIdKey()

	c.Send("MULTI")
	c.Send("HSET", recordHashKey, recordHashField, jsonData)
	c.Send("HSET", indexHashKey, indexHashField, id)
	c.Send("INCR", maxIdKey)
	ret, err := c.Do("EXEC")
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return 0, err
	}

	debugPrintf("Create(): ok: %v\n", ret)

	return id, nil
}

func (db *DB) BatchCreate(c redis.Conn, jsonDataArr []string) (ids []uint64, err error) {
	ids = []uint64{}
	var id uint64 = 0

	for _, v := range jsonDataArr {
		if id, err = db.Create(c, v); err != nil {
			debugPrintf("BatchCreate(): error: %v\n", err)
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (db *DB) Search(c redis.Conn, pattern string) (ids []string, err error) {
	if len(pattern) == 0 {
		err = errors.New("Empty pattern.")
		debugPrintf("Search() error: %v\n", err)
		return []string{}, err
	}

	maxBucketId, err := db.GetMaxBucketId(c)
	if err != nil {
		debugPrintf("Search() error: %v\n", err)
		return []string{}, err
	}

	indexHashKey := ""
	ids = []string{}
	items := []string{}

	for i := maxBucketId; i >= 1; i-- {
		_, indexHashKey = db.GenHashKey(i)
		cursor := 0
		for {
			v, err := redis.Values(c.Do("HSCAN", indexHashKey, cursor, "match", pattern))
			if err != nil {
				debugPrintf("Search(): HSCAN error: %v\n", err)
				return []string{}, err
			}

			v, err = redis.Scan(v, &cursor, &items)
			if err != nil {
				fmt.Printf("err: %v\n", err)
				return []string{}, err
			}

			l := len(items)
			if l > 0 && l%2 == 0 {
				for m := 1; m < l; m += 2 {
					ids = append(ids, items[m])
				}
			}

			if cursor == 0 {
				break
			}
		}
	}
	return ids, nil
}
