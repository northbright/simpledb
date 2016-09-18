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

func (db *DB) FindIndexHashKey(c redis.Conn, data string) (indexHashKey string, err error) {
	maxBucketId, err := db.GetMaxBucketId(c)
	if err != nil {
		debugPrintf("GetMaxBucketId() error: %v\n", err)
		return "", err
	}

	indexHashKey = ""
	indexHashField := data

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

func (db *DB) Create(c redis.Conn, data string) (id string, err error) {
	// 1. Check json data.
	if len(data) == 0 {
		err = errors.New("Empty data.")
		debugPrintf("Create() error: %v\n", err)
		return "", err
	}

	// 2. Get max id and compute current id.
	maxId, err := db.GetMaxId(c)
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return "", err
	}

	nId := maxId + 1

	// 3. Compute current bucket id.
	//    Increase max bucket id if current bucket id > max bucket id.
	bucketId := ComputeBucketId(nId)

	maxBucketId, err := db.GetMaxBucketId(c)
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return "", err
	}

	if bucketId > maxBucketId {
		k := db.GenMaxBucketIdKey()
		_, err := c.Do("INCR", k)
		if err != nil {
			debugPrintf("Create() error: %v\n", err)
			return "", err
		}
	}

	// 4. Generate hash key for record and index.
	recordHashKey, indexHashKey := db.GenHashKey(bucketId)

	// 5. Check if json data already exists.
	oldIndexHashKey, err := db.FindIndexHashKey(c, data)
	if err != nil {
		debugPrintf("Create(): error: %v\n", err)
		return "", err
	}

	// Index already exists, it means the record also exists.
	if oldIndexHashKey != "" {
		err = errors.New("JSON data already exists.")
		debugPrintf("Create(): error: %v\n", err)
		return "", err
	}

	// 6. Create record and index
	recordHashField := nId
	indexHashField := data
	maxIdKey := db.GenMaxIdKey()

	c.Send("MULTI")
	c.Send("HSET", recordHashKey, recordHashField, data)
	c.Send("HSET", indexHashKey, indexHashField, nId)
	c.Send("INCR", maxIdKey)
	ret, err := c.Do("EXEC")
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return "", err
	}

	debugPrintf("Create(): ok: %v\n", ret)

	id = strconv.FormatUint(nId, 10)
	return id, nil
}

func (db *DB) BatchCreate(c redis.Conn, dataArr []string) (ids []string, err error) {
	ids = []string{}
	id := ""

	for _, v := range dataArr {
		if id, err = db.Create(c, v); err != nil {
			debugPrintf("BatchCreate(): error: %v\n", err)
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (db *DB) Exists(c redis.Conn, id string) (exists bool, recordHashKey string, recordHashField uint64, err error) {
	nId, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		debugPrintf("Exists() strconv.ParseUint() error: %v\n", err)
		return false, "", 0, err
	}

	bucketId := ComputeBucketId(nId)
	recordHashKey, _ = db.GenHashKey(bucketId)
	recordHashField = nId

	exists, err = redis.Bool(c.Do("HEXISTS", recordHashKey, recordHashField))
	if err != nil {
		debugPrintf("Exists() error: %v\n", err)
		return false, "", 0, err
	}

	return exists, recordHashKey, recordHashField, nil
}

func (db *DB) Get(c redis.Conn, id string) (data string, err error) {
	exists, recordHashKey, recordHashField, err := db.Exists(c, id)
	if err != nil {
		debugPrintf("Get(): db.Exists() error: %v\n", err)
		return "", err
	}

	if !exists {
		err = errors.New("Record filed does not exists in hash key.")
		debugPrintf("Get(): error: %v\n", err)
		return "", err
	}

	data, err = redis.String(c.Do("HGET", recordHashKey, recordHashField))
	if err != nil {
		debugPrintf("Get(): error: %v\n", err)
		return "", err
	}

	return data, nil
}

func (db *DB) BatchGet(c redis.Conn, ids []string) (dataMap map[string]string, err error) {
	dataMap = make(map[string]string)
	data := ""

	for _, id := range ids {
		data, err = db.Get(c, id)
		if err != nil {
			debugPrintf("BatchGet(): db.Get() error: %v\n", err)
			return dataMap, err
		}
		dataMap[id] = data
	}
	return dataMap, nil
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
