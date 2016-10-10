package simpledb

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"regexp"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

const (
	EstimatedMaxRecordNum uint64 = 1000000
)

var (
	DEBUG                         bool   = true
	DefRedisHashMaxZiplistEntries uint64 = 512
)

type DB struct {
	Name                       string
	redisHashMaxZiplistEntries uint64
	estIndexBucketNum          uint64
	indexHashKeyScanPattern    string
}

func (db *DB) GenRedisHashMaxZiplistEntriesKey() (redisHashMaxZiplistEntriesKey string) {
	return fmt.Sprintf("%v/redis-hash-max-ziplist-entries", db.Name)
}

func Open(c redis.Conn, name string) (db *DB, err error) {
	db = &DB{Name: name}
	exists := false
	k := db.GenRedisHashMaxZiplistEntriesKey()

	if len(name) == 0 {
		err = errors.New("Empty db name.")
		goto end
	}

	exists, err = redis.Bool(c.Do("EXISTS", k))
	if err != nil {
		goto end
	}

	// Set db.redisHashMaxZiplistEntries only once at first time.
	if !exists {
		db.redisHashMaxZiplistEntries = GetRedisHashMaxZiplistEntries(c)
		if err != nil {
			goto end
		}

		_, err = c.Do("SET", k, db.redisHashMaxZiplistEntries)
		if err != nil {
			goto end
		}
	} else {
		db.redisHashMaxZiplistEntries, err = redis.Uint64(c.Do("GET", k))
		if err != nil {
			goto end
		}
	}

	db.estIndexBucketNum = EstimatedMaxRecordNum / uint64(float64(db.redisHashMaxZiplistEntries)*0.9)
	db.indexHashKeyScanPattern = fmt.Sprintf("%v/idx/bucket/*", db.Name)
end:
	if err != nil {
		DebugPrintf("Open(c, %v) error: %v\n", name, err)
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() {
}

func (db *DB) ComputeBucketId(id uint64) uint64 {
	return id/db.redisHashMaxZiplistEntries + 1
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
		DebugPrintf("GenMaxId() error: %v\n", err)
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
		DebugPrintf("GetMaxBucketId() error: %v\n", err)
		return 1, err
	}

	return maxBucketId, nil
}

func (db *DB) GenRecordHashKey(id uint64) string {
	bucketId := db.ComputeBucketId(id)
	return fmt.Sprintf("%v/bucket/%v", db.Name, bucketId)
}

func (db *DB) GenIndexHashKey(data string) string {
	checkSum := crc32.ChecksumIEEE([]byte(data))
	bucketId := uint64(checkSum) % db.estIndexBucketNum
	return fmt.Sprintf("%v/idx/bucket/%v", db.Name, bucketId)
}

func (db *DB) Exists(c redis.Conn, data string) (exists bool, err error) {
	exists = false
	indexHashKey := ""
	indexHashField := data

	indexHashKey = db.GenIndexHashKey(data)
	exists, err = redis.Bool(c.Do("HEXISTS", indexHashKey, indexHashField))
	if err != nil {
		goto end
	}

end:
	if err != nil {
		DebugPrintf("Exists() error: %v\n", err)
		return false, err
	}

	return exists, nil
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
		DebugPrintf("Create() error: %v\n", err)
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
		exists, err = db.Exists(c, data)
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

	// Get max bucket id key.
	maxBucketIdKey = db.GenMaxBucketIdKey()
	if err != nil {
		goto end
	}

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
		bucketId = db.ComputeBucketId(nId)

		// Generate hash key for record and index.
		recordHashKey = db.GenRecordHashKey(nId)
		indexHashKey = db.GenIndexHashKey(data)

		// Create record and index.
		recordHashField = nId
		indexHashField = data

		c.Send("HSET", recordHashKey, recordHashField, data)
		c.Send("HSET", indexHashKey, indexHashField, nId)
	}

	// Increase max bucket id if need.
	if bucketId > maxBucketId {
		c.Send("SET", maxBucketIdKey, bucketId)
	}

	c.Send("INCRBY", maxIdKey, len(dataArr))

	// Do piplined transaction.
	ret, err = c.Do("EXEC")
	if err != nil {
		goto end
	}

	DebugPrintf("BatchCreate() ok. ret: %v, ids: %v\n", ret, ids)

end:
	if err != nil {
		if alreadySendMULTI {
			c.Do("DISCARD")
		}
		DebugPrintf("BatchCreate() error: %v\n", err)
		return []string{}, err
	}

	return ids, nil
}

func (db *DB) IdExists(c redis.Conn, id string) (exists bool, err error) {
	var nId uint64
	var recordHashKey string

	nId, err = strconv.ParseUint(id, 10, 64)
	if err != nil {
		goto end
	}

	recordHashKey = db.GenRecordHashKey(nId)
	exists, err = redis.Bool(c.Do("HEXISTS", recordHashKey, nId))
	if err != nil {
		goto end
	}

end:
	if err != nil {
		DebugPrintf("IdExists()  error: %v\n", err)
		return false, err
	}

	return exists, nil
}

func (db *DB) Get(c redis.Conn, id string) (data string, err error) {
	var nId uint64
	recordHashKey := ""

	nId, err = strconv.ParseUint(id, 10, 64)
	if err != nil {
		goto end
	}

	recordHashKey = db.GenRecordHashKey(nId)
	data, err = redis.String(c.Do("HGET", recordHashKey, nId))
	if err != nil {
		goto end
	}

end:
	if err != nil {
		DebugPrintf("Get() error: %v\n", err)
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
			DebugPrintf("BatchGet(): db.Get() error: %v\n", err)
			return dataMap, err
		}
		dataMap[id] = data
	}
	return dataMap, nil
}

func (db *DB) Update(c redis.Conn, id, data string) error {
	dataMap := make(map[string]string)
	dataMap[id] = data
	return db.BatchUpdate(c, dataMap)
}

func (db *DB) BatchUpdate(c redis.Conn, dataMap map[string]string) (err error) {
	type updateInfo struct {
		data              string
		recordHashKey     string
		recordHashField   uint64
		oldIndexHashKey   string
		oldIndexHashField string
		newIndexHashKey   string
		newIndexHashField string
	}

	var recordHashKey, oldIndexHashKey, newIndexHashKey, oldData string
	var nId, recordHashField uint64
	var ret interface{}
	exists := false
	updateInfoMap := make(map[uint64]updateInfo)
	alreadySendMULTI := false

	// Check and input dataArr
	for id, data := range dataMap {
		exists, err = db.IdExists(c, id)
		if err != nil {
			goto end
		}

		if !exists {
			err = errors.New(fmt.Sprintf("Id: %v does not exist.", id))
			goto end
		}

		oldData, err = db.Get(c, id)
		if err != nil {
			goto end
		}

		nId, err = strconv.ParseUint(id, 10, 64)
		if err != nil {
			goto end
		}

		recordHashKey = db.GenRecordHashKey(nId)
		recordHashField = nId
		oldIndexHashKey = db.GenIndexHashKey(oldData)
		newIndexHashKey = db.GenIndexHashKey(data)

		// Check if new data equals to old data: no need to update.
		if !bytes.Equal([]byte(data), []byte(oldData)) {
			updateInfoMap[recordHashField] = updateInfo{
				data:              data,
				recordHashKey:     recordHashKey,
				recordHashField:   recordHashField,
				oldIndexHashKey:   oldIndexHashKey,
				oldIndexHashField: oldData,
				newIndexHashKey:   newIndexHashKey,
				newIndexHashField: data}
		}

		// Check if data already exists in db(may be with another id).
		exists, err = db.Exists(c, data)
		if err != nil {
			goto end
		}

		if exists {
			err = errors.New(fmt.Sprintf("Data already exists in db: %v.", data))
			goto end
		}
	}

	// Prepare pipelined transaction.
	c.Send("MULTI")
	alreadySendMULTI = true

	for nId, info := range updateInfoMap {
		c.Send("HSET", info.recordHashKey, info.recordHashField, info.data)
		c.Send("HSET", info.newIndexHashKey, info.newIndexHashField, nId)
		c.Send("HDEL", info.oldIndexHashKey, info.oldIndexHashField)
	}

	ret, err = c.Do("EXEC")
	if err != nil {
		goto end
	}

	DebugPrintf("BatchUpdate() ok. ret: %v\n", ret)

end:
	if err != nil {
		if alreadySendMULTI {
			c.Do("DISCARD")
		}
		DebugPrintf("BatchUpdate() error: %v\n", err)
		return err
	}

	return nil
}

func (db *DB) Delete(c redis.Conn, id string) (err error) {
	return db.BatchDelete(c, []string{id})
}

func (db *DB) BatchDelete(c redis.Conn, ids []string) (err error) {
	type delInfo struct {
		recordHashKey   string
		recordHashField uint64
		indexHashKey    string
		indexHashField  string
	}

	var recordHashKey, indexHashKey, data string
	var nId, recordHashField uint64
	var ret interface{}
	delInfoMap := make(map[uint64]delInfo)
	exists := false
	alreadySendMULTI := false

	// Check Id
	for _, id := range ids {
		exists, err = db.IdExists(c, id)
		if err != nil {
			goto end
		}

		if !exists {
			err = errors.New(fmt.Sprintf("id:%v does not exist.", id))
			goto end
		}

		data, err = db.Get(c, id)
		if err != nil {
			goto end
		}

		nId, err = strconv.ParseUint(id, 10, 64)
		if err != nil {
			goto end
		}

		recordHashKey = db.GenRecordHashKey(nId)
		recordHashField = nId
		indexHashKey = db.GenIndexHashKey(data)

		delInfoMap[recordHashField] = delInfo{
			recordHashKey:   recordHashKey,
			recordHashField: recordHashField,
			indexHashKey:    indexHashKey,
			indexHashField:  data,
		}
	}

	// Prepare pipelined transaction.
	c.Send("MULTI")
	alreadySendMULTI = true

	for _, info := range delInfoMap {
		c.Send("HDEL", info.recordHashKey, info.recordHashField)
		c.Send("HDEL", info.indexHashKey, info.indexHashField)
	}

	ret, err = c.Do("EXEC")
	if err != nil {
		goto end
	}

	DebugPrintf("BatchDelete() ok. ret: %v\n", ret)

end:
	if err != nil {
		if alreadySendMULTI {
			c.Do("DISCARD")
		}
		DebugPrintf("BatchDelete() error: %v\n", err)
		return err
	}

	return nil

}

func (db *DB) Search(c redis.Conn, pattern string) (ids []string, err error) {
	var cursor, subCursor uint64
	var l int = 0
	var v []interface{}
	keys := []string{}
	items := []string{}
	ids = []string{}

	if len(pattern) == 0 {
		err = errors.New("Empty pattern.")
		goto end
	}

	cursor = 0
	for {
		v, err = redis.Values(c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024))
		if err != nil {
			goto end
		}

		v, err = redis.Scan(v, &cursor, &keys)
		if err != nil {
			goto end
		}

		for _, k := range keys {
			subCursor = 0
			for {
				v, err = redis.Values(c.Do("HSCAN", k, subCursor, "match", pattern, "COUNT", 1024))
				if err != nil {
					goto end
				}

				v, err = redis.Scan(v, &subCursor, &items)
				if err != nil {
					goto end
				}

				l = len(items)
				if l > 0 {
					if l%2 != 0 {
						errors.New("Search() error: HSCAN result error.")
						goto end
					}

					for m := 1; m < l; m += 2 {
						ids = append(ids, items[m])
					}
				}

				if subCursor == 0 {
					break
				}
			}
		}

		if cursor == 0 {
			break
		}
	}
end:
	if err != nil {
		DebugPrintf("Search() error: %v\n", err)
		return []string{}, err
	}

	return ids, nil
}

func (db *DB) RegexpSearch(c redis.Conn, patterns []string) (ids [][]string, err error) {
	var cursor, subCursor uint64
	var l int = 0
	var v []interface{}
	keys := []string{}
	items := []string{}
	reArr := []*regexp.Regexp{}

	for _, p := range patterns {
		reArr = append(reArr, regexp.MustCompile(p))
		ids = append(ids, []string{})
	}

	cursor = 0
	for {
		v, err = redis.Values(c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024))
		if err != nil {
			goto end
		}

		v, err = redis.Scan(v, &cursor, &keys)
		if err != nil {
			goto end
		}

		for _, k := range keys {
			subCursor = 0
			for {
				v, err = redis.Values(c.Do("HSCAN", k, subCursor, "COUNT", 1024))
				if err != nil {
					goto end
				}

				v, err = redis.Scan(v, &subCursor, &items)
				if err != nil {
					goto end
				}

				l = len(items)
				if l > 0 {
					if l%2 != 0 {
						errors.New("Search() error: HSCAN result error.")
						goto end
					}

					for m := 1; m < l; m += 2 {
						for n, re := range reArr {
							if re.MatchString(items[m-1]) {
								ids[n] = append(ids[n], items[m])
							}
						}
					}
				}

				if subCursor == 0 {
					break
				}
			}
		}

		if cursor == 0 {
			break
		}
	}
end:
	if err != nil {
		DebugPrintf("Search() error: %v\n", err)
		return [][]string{}, err
	}

	return ids, nil
}

func (db *DB) Info(c redis.Conn) (infoMap map[string]string, err error) {
	var maxId, maxBucketId, recordBucketNum, recordNum, indexBucketNum, indexNum, n, cursor uint64
	var recordHashKey string
	ret := ""
	encoding := ""
	encodingPattern := `(ziplist|hashtable)`
	re := regexp.MustCompile(encodingPattern)
	allRecordBucketEncodingAreZipList := true
	allIndexBucketEncodingAreZipList := true
	hashTableEncodingRecordHashKeys := []string{}
	hashTableEncodingIndexHashKeys := []string{}
	keys := []string{}
	infoMap = make(map[string]string)
	var v []interface{}

	maxId, err = db.GetMaxId(c)
	if err != nil {
		goto end
	}
	infoMap["max id"] = strconv.FormatUint(maxId, 10)

	maxBucketId, err = db.GetMaxBucketId(c)
	if err != nil {
		goto end
	}
	infoMap["max bucket id"] = strconv.FormatUint(maxBucketId, 10)

	// Check record hashes' encoding
	for i := maxBucketId; i >= 1; i-- {
		recordHashKey = fmt.Sprintf("%v/bucket/%v", db.Name, i)
		n, err = redis.Uint64(c.Do("HLEN", recordHashKey))
		if err != nil {
			goto end
		}

		if n > 0 {
			recordBucketNum += 1
			recordNum += n

			// Check hash encoding: ziplist or hashtable.
			ret, err = redis.String(c.Do("DEBUG", "OBJECT", recordHashKey))
			if err != nil {
				DebugPrintf("failed to exec debug object, %v\n.", recordHashKey)
				goto end
			}

			encoding = re.FindString(ret)
			if encoding == "" {
				err = errors.New(fmt.Sprintf("Can not find encoding of %v.", recordHashKey))
				goto end
			}

			if encoding == "hashtable" {
				hashTableEncodingRecordHashKeys = append(hashTableEncodingRecordHashKeys, recordHashKey)
			}
		}
	}

	// Check index hashes' encoding
	cursor = 0
	for {
		v, err = redis.Values(c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024))
		if err != nil {
			goto end
		}

		v, err = redis.Scan(v, &cursor, &keys)
		if err != nil {
			goto end
		}

		for _, k := range keys {
			indexBucketNum += 1

			n, err = redis.Uint64(c.Do("HLEN", k))
			if err != nil {
				goto end
			}
			indexNum += n

			// Check hash encoding: ziplist or hashtable
			ret, err = redis.String(c.Do("DEBUG", "OBJECT", k))
			if err != nil {
				goto end
			}

			encoding = re.FindString(ret)
			if encoding == "" {
				err = errors.New(fmt.Sprintf("Can not find encoding of %v.", recordHashKey))
				goto end
			}

			if encoding == "hashtable" {
				hashTableEncodingIndexHashKeys = append(hashTableEncodingIndexHashKeys, k)
			}

		}

		if cursor == 0 {
			break
		}
	}

	if len(hashTableEncodingRecordHashKeys) > 0 {
		allRecordBucketEncodingAreZipList = false
	}

	if len(hashTableEncodingIndexHashKeys) > 0 {
		allIndexBucketEncodingAreZipList = false
	}

	infoMap["db.Name"] = db.Name
	infoMap["db.redisHashMaxZiplistEntries"] = strconv.FormatUint(db.redisHashMaxZiplistEntries, 10)
	infoMap["record bucket num"] = strconv.FormatUint(recordBucketNum, 10)
	infoMap["record num"] = strconv.FormatUint(recordNum, 10)
	infoMap["index bucket num"] = strconv.FormatUint(indexBucketNum, 10)
	infoMap["index num"] = strconv.FormatUint(indexNum, 10)
	infoMap["all record bucket encoding are 'ziplist'"] = fmt.Sprintf("%v", allRecordBucketEncodingAreZipList)
	infoMap["all index bucket encoding are 'ziplist'"] = fmt.Sprintf("%v", allIndexBucketEncodingAreZipList)
	infoMap[fmt.Sprintf("hashtable encoding record hash keys(%v)", len(hashTableEncodingRecordHashKeys))] = fmt.Sprintf("%v", hashTableEncodingRecordHashKeys)
	infoMap[fmt.Sprintf("hashtable encoding index hash keys(%v)", len(hashTableEncodingIndexHashKeys))] = fmt.Sprintf("%v", hashTableEncodingIndexHashKeys)

end:
	if err != nil {
		DebugPrintf("Info() error: %v\n", err)
		return make(map[string]string), err
	}
	return infoMap, nil
}
