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
		goto end
	}

	if !exists {
		maxId = 0
		goto end
	}

	maxId, err = redis.Uint64(c.Do("GET", k))
	if err != nil {
		goto end
	}

end:
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
		debugPrintf("XX error: %v\n", err)
		goto end
	}

	if !exists {
		_, err := c.Do("SET", k, 1)
		if err != nil {
			goto end
		}
	}

	maxBucketId, err = redis.Uint64(c.Do("GET", k))
	if err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("GetMaxBucketId() error: %v\n", err)
		return 1, err
	}
	return maxBucketId, nil
}

func (db *DB) GenHashKey(bucketId uint64) (recordHashKey, indexHashKey string) {
	bucketIdStr := strconv.FormatUint(bucketId, 10)
	recordHashKey = fmt.Sprintf("%v/bucket/%v", db.Name, bucketIdStr)
	indexHashKey = fmt.Sprintf("%v/idx/bucket/%v", db.Name, bucketIdStr)
	return recordHashKey, indexHashKey
}

func (db *DB) IndexExists(c redis.Conn, data string) (exists bool, err error) {
	maxBucketId, err := db.GetMaxBucketId(c)
	if err != nil {
		debugPrintf("GetMaxBucketId() error: %v\n", err)
		return false, err
	}

	indexHashKey := ""
	indexHashField := data

	for i := maxBucketId; i >= 1; i-- {
		_, indexHashKey = db.GenHashKey(i)
		exists, err := redis.Bool(c.Do("HEXISTS", indexHashKey, indexHashField))
		if err != nil {
			debugPrintf("IndexExists() error: %v\n", err)
			return false, err
		}

		if exists {
			return true, nil
		}
	}
	return false, nil
}

func (db *DB) Create(c redis.Conn, data string) (id string, err error) {
	ids := []string{}
	ids, err = db.BatchCreate(c, []string{data})
	if err != nil {
		goto end
	}

	if len(ids) != 1 {
		err = errors.New("Count of created record != 1.")
		goto end
	}

end:
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return "", err
	}

	return ids[0], nil
}

func (db *DB) BatchCreate(c redis.Conn, dataArr []string) (ids []string, err error) {
	var checkedData map[string]int = make(map[string]int) // key: data, value: order in dataArr.
	var nId, maxId, bucketId, maxBucketId, recordHashField uint64
	var maxBucketIdKey, recordHashKey, indexHashKey, indexHashField, maxIdKey string
	var ret interface{}
	err = nil
	ids = []string{}
	id := ""
	ok := false
	exists := false
	alreadySendMULTI := false

	// Check data.
	for i, data := range dataArr {
		// Check empty data.
		if len(data) == 0 {
			err = errors.New("Empty data.")
			goto end
		}

		// Check redundant data in dataArr.
		if _, ok = checkedData[data]; ok {
			err = errors.New(fmt.Sprintf("Redundant data found in dataArr: %v", data))
			goto end
		}
		checkedData[data] = i

		// Check if data already exist in db.
		exists, err = db.IndexExists(c, data)
		if err != nil {
			goto end
		}

		if exists {
			err = errors.New(fmt.Sprintf("Data already exists in db: %v.", data))
			goto end
		}
	}

	// Get max id.
	maxId, err = db.GetMaxId(c)
	if err != nil {
		goto end
	}

	// Get max id key.
	maxIdKey = db.GenMaxIdKey()

	// Get max bucket id.
	maxBucketId, err = db.GetMaxBucketId(c)
	if err != nil {
		goto end
	}

	// Prepare piplined transaction.
	c.Send("MULTI")
	alreadySendMULTI = true

	for i, data := range dataArr {
		// Increase Id
		nId = maxId + uint64(i+1)
		id = strconv.FormatUint(nId, 10)
		// Insert to result id array
		ids = append(ids, id)

		// Compute bucket id.
		bucketId = ComputeBucketId(nId)

		// Increase max bucket id if need.
		if bucketId > maxBucketId {
			c.Send("SET", maxBucketIdKey, bucketId)
		}

		// Generate hash key for record and index.
		recordHashKey, indexHashKey = db.GenHashKey(bucketId)

		// Create record and index.
		recordHashField = nId
		indexHashField = data

		c.Send("HSET", recordHashKey, recordHashField, data)
		c.Send("HSET", indexHashKey, indexHashField, nId)
		c.Send("INCR", maxIdKey)
	}

	// Do piplined transaction.
	ret, err = c.Do("EXEC")
	if err != nil {
		goto end
	}

	debugPrintf("BatchCreate() ok. ret: %v, ids: %v\n", ret, ids)

end:
	if err != nil {
		if alreadySendMULTI {
			c.Do("DISCARD")
		}
		debugPrintf("BatchCreate() error: %v\n", err)
		return []string{}, err
	}

	return ids, nil
}

func (db *DB) Exists(c redis.Conn, id string) (exists bool, recordHashKey, indexHashKey string, recordHashField uint64, err error) {
	nId, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		debugPrintf("Exists() strconv.ParseUint() error: %v\n", err)
		return false, "", "", 0, err
	}

	bucketId := ComputeBucketId(nId)
	recordHashKey, indexHashKey = db.GenHashKey(bucketId)
	recordHashField = nId

	exists, err = redis.Bool(c.Do("HEXISTS", recordHashKey, recordHashField))
	if err != nil {
		debugPrintf("Exists() error: %v\n", err)
		return false, "", "", 0, err
	}

	return exists, recordHashKey, indexHashKey, recordHashField, nil
}

func (db *DB) Get(c redis.Conn, id string) (data string, err error) {
	exists, recordHashKey, _, recordHashField, err := db.Exists(c, id)
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

func (db *DB) Update(c redis.Conn, id, data string) error {
	exists, recordHashKey, indexHashKey, recordHashField, err := db.Exists(c, id)
	if err != nil {
		debugPrintf("Update() db.Exists() error: %v\n", err)
		return err
	}

	if !exists {
		err = errors.New("Record does not exist.")
		debugPrintf("Update() error: %v\n", err)
		return err
	}

	oldData, err := db.Get(c, id)
	if err != nil {
		debugPrintf("Update() db.Get() error: %v\n", err)
		return err
	}

	oldIndexHashField := oldData
	newIndexHashField := data
	nId := recordHashField

	// Check if data already exists.
	exists, err = db.IndexExists(c, data)
	if err != nil {
		debugPrintf("Update() error: %v\n", err)
		return err
	}

	// Index already exists, it means the there's already a record has the same value / index.
	if exists {
		err = errors.New("Same data / index already exists.")
		debugPrintf("Update() error: %v\n", err)
		return err
	}

	c.Send("MULTI")
	c.Send("HSET", recordHashKey, recordHashField, data)
	c.Send("HSET", indexHashKey, newIndexHashField, nId)
	c.Send("HDEL", indexHashKey, oldIndexHashField)
	ret, err := c.Do("EXEC")
	if err != nil {
		debugPrintf("Update() error: %v\n", err)
		return err
	}

	debugPrintf("Update() ok. ret: %v\n", ret)

	return nil
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
