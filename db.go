package simpledb

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
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
		DebugPrintf("XX error: %v\n", err)
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

func (db *DB) GenHashKey(bucketId uint64) (recordHashKey, indexHashKey string) {
	bucketIdStr := strconv.FormatUint(bucketId, 10)
	recordHashKey = fmt.Sprintf("%v/bucket/%v", db.Name, bucketIdStr)
	indexHashKey = fmt.Sprintf("%v/idx/bucket/%v", db.Name, bucketIdStr)
	return recordHashKey, indexHashKey
}

func (db *DB) Exists(c redis.Conn, data string) (exists bool, err error) {
	exists = false
	maxBucketId, err := db.GetMaxBucketId(c)
	indexHashKey := ""
	indexHashField := data

	if err != nil {
		goto end
	}

	for i := maxBucketId; i >= 1; i-- {
		_, indexHashKey = db.GenHashKey(i)
		exists, err = redis.Bool(c.Do("HEXISTS", indexHashKey, indexHashField))
		if err != nil {
			goto end
		}

		if exists {
			exists = true
			goto end
		}
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

func (db *DB) IdExists(c redis.Conn, id string) (exists bool, recordHashKey, indexHashKey string, recordHashField uint64, err error) {
	var nId, bucketId uint64

	nId, err = strconv.ParseUint(id, 10, 64)
	if err != nil {
		goto end
	}

	bucketId = ComputeBucketId(nId)
	recordHashKey, indexHashKey = db.GenHashKey(bucketId)
	recordHashField = nId

	exists, err = redis.Bool(c.Do("HEXISTS", recordHashKey, recordHashField))
	if err != nil {
		goto end
	}

end:
	if err != nil {
		DebugPrintf("IdExists()  error: %v\n", err)
		return false, "", "", 0, err
	}

	return exists, recordHashKey, indexHashKey, recordHashField, nil
}

func (db *DB) Get(c redis.Conn, id string) (data string, err error) {
	exists, recordHashKey, _, recordHashField, err := db.IdExists(c, id)
	if err != nil {
		goto end
	}

	if !exists {
		err = errors.New("Record filed does not exists in hash key.")
		goto end
	}

	data, err = redis.String(c.Do("HGET", recordHashKey, recordHashField))
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
		indexHashKey      string
		oldIndexHashField string
		newIndexHashField string
	}

	var recordHashKey, indexHashKey, oldData string
	var recordHashField uint64
	var ret interface{}
	exists := false
	updateInfoMap := make(map[uint64]updateInfo)
	alreadySendMULTI := false

	// Check and input dataArr
	for id, data := range dataMap {
		exists, recordHashKey, indexHashKey, recordHashField, err = db.IdExists(c, id)
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

		// Check if new data equals to old data: no need to update.
		if !bytes.Equal([]byte(data), []byte(oldData)) {
			updateInfoMap[recordHashField] = updateInfo{
				data:              data,
				recordHashKey:     recordHashKey,
				recordHashField:   recordHashField,
				indexHashKey:      indexHashKey,
				oldIndexHashField: oldData,
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
		c.Send("HSET", info.indexHashKey, info.newIndexHashField, nId)
		c.Send("HDEL", info.indexHashKey, info.oldIndexHashField)
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
	var recordHashField uint64
	var ret interface{}
	delInfoMap := make(map[uint64]delInfo)
	exists := false
	alreadySendMULTI := false

	// Check Id
	for _, id := range ids {
		exists, recordHashKey, indexHashKey, recordHashField, err = db.IdExists(c, id)
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
	var maxBucketId, cursor uint64
	var l int = 0
	var v []interface{}
	indexHashKey := ""
	items := []string{}
	ids = []string{}

	if len(pattern) == 0 {
		err = errors.New("Empty pattern.")
		goto end
	}

	maxBucketId, err = db.GetMaxBucketId(c)
	if err != nil {
		goto end
	}

	for i := maxBucketId; i >= 1; i-- {
		_, indexHashKey = db.GenHashKey(i)
		cursor = 0
		for {
			v, err = redis.Values(c.Do("HSCAN", indexHashKey, cursor, "match", pattern))
			if err != nil {
				goto end
			}

			v, err = redis.Scan(v, &cursor, &items)
			if err != nil {
				goto end
			}

			l = len(items)
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
end:
	if err != nil {
		DebugPrintf("Search() error: %v\n", err)
		return []string{}, err
	}

	return ids, nil
}

func (db *DB) Info(c redis.Conn) (infoMap map[string]string, err error) {
	var maxId, maxBucketId, recordBucketNum, recordNum, indexBucketNum, indexNum, n uint64
	var recordHashKey, indexHashKey string
	ret := ""
	encoding := ""
	encodingPattern := `(ziplist|hashtable)`
	re := regexp.MustCompile(encodingPattern)
	allRecordBucketEncodingAreZipList := true
	allIndexBucketEncodingAreZipList := true
	hashTableEncodingRecordHashKeys := []string{}
	hashTableEncodingIndexHashKeys := []string{}

	infoMap = make(map[string]string)

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

	for i := maxBucketId; i >= 1; i-- {
		recordHashKey, indexHashKey = db.GenHashKey(i)
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

		n, err = redis.Uint64(c.Do("HLEN", indexHashKey))
		if err != nil {
			goto end
		}

		if n > 0 {
			indexBucketNum += 1
			indexNum += n

			// Check hash encoding: ziplist or hashtable
			ret, err = redis.String(c.Do("DEBUG", "OBJECT", indexHashKey))
			if err != nil {
				goto end
			}

			encoding = re.FindString(ret)
			if encoding == "" {
				err = errors.New(fmt.Sprintf("Can not find encoding of %v.", recordHashKey))
				goto end
			}

			if encoding == "hashtable" {
				hashTableEncodingIndexHashKeys = append(hashTableEncodingIndexHashKeys, indexHashKey)
			}

		}
	}

	if len(hashTableEncodingRecordHashKeys) > 0 {
		allRecordBucketEncodingAreZipList = false
	}

	if len(hashTableEncodingIndexHashKeys) > 0 {
		allIndexBucketEncodingAreZipList = false
	}

	infoMap["record bucket num"] = strconv.FormatUint(recordBucketNum, 10)
	infoMap["record num"] = strconv.FormatUint(recordNum, 10)
	infoMap["index bucket num"] = strconv.FormatUint(indexBucketNum, 10)
	infoMap["index num"] = strconv.FormatUint(indexNum, 10)
	infoMap["all record bucket encoding are 'ziplist'"] = fmt.Sprintf("%v", allRecordBucketEncodingAreZipList)
	infoMap["all index bucket encoding are 'ziplist'"] = fmt.Sprintf("%v", allIndexBucketEncodingAreZipList)
	infoMap["hashtable encoding record hash keys"] = fmt.Sprintf("%v", hashTableEncodingRecordHashKeys)
	infoMap["hashtable encoding index hash keys"] = fmt.Sprintf("%v", hashTableEncodingIndexHashKeys)

end:
	if err != nil {
		DebugPrintf("Info() error: %v\n", err)
		return make(map[string]string), err
	}
	return infoMap, nil
}
