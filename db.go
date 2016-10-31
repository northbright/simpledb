package simpledb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"regexp"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

const (
	// EstimatedMaxRecordNum is estimated max record number.
	EstimatedMaxRecordNum uint64 = 1000000
)

var (
	// DEBUG represents debug mode. It'll output debug messages if it's true.
	DEBUG = true
)

// DB represents a record collection stored in Redis server.
type DB struct {
	// Database name.
	name string
	// Redis "hash-max-ziplist-entries" value. It'll be initialize only once in Open().
	redisHashMaxZiplistEntries uint64
	// Estimated index bucket number.
	estIndexBucketNum uint64
	// Index hash key scan pattern. It's used to scan index entries in Redis.
	indexHashKeyScanPattern string
}

// Record contains record ID and data string.
type Record struct {
	// ID is record ID.
	ID string
	// data is record dta.
	Data string
}

// GenRedisHashMaxZiplistEntriesKey Generates the "hash-max-ziplist-entries" key for DB.
func (db *DB) GenRedisHashMaxZiplistEntriesKey() (redisHashMaxZiplistEntriesKey string) {
	return fmt.Sprintf("%v/redis-hash-max-ziplist-entries", db.name)
}

// Open returns an DB instance by given database name.
func Open(c redis.Conn, name string) (db *DB, err error) {
	db = &DB{name: name}
	exists := false
	k := db.GenRedisHashMaxZiplistEntriesKey()

	if len(name) == 0 {
		err = fmt.Errorf("Empty db name.")
		goto end
	}

	if exists, err = redis.Bool(c.Do("EXISTS", k)); err != nil {
		goto end
	}

	// Set db.redisHashMaxZiplistEntries only once at first time.
	if !exists {
		if db.redisHashMaxZiplistEntries, err = GetRedisHashMaxZiplistEntries(c); err != nil {
			goto end
		}

		if _, err = c.Do("SET", k, db.redisHashMaxZiplistEntries); err != nil {
			goto end
		}
	} else {
		if db.redisHashMaxZiplistEntries, err = redis.Uint64(c.Do("GET", k)); err != nil {
			goto end
		}
	}

	// Initialize estimated index bucket number.
	db.estIndexBucketNum = EstimatedMaxRecordNum / uint64(float64(db.redisHashMaxZiplistEntries)*0.9)
	// Initialize index hash key scan pattern.
	db.indexHashKeyScanPattern = fmt.Sprintf("%v/idx/bucket/*", db.name)
end:
	if err != nil {
		debugPrintf("Open(c, %v) error: %v\n", name, err)
		return nil, err
	}

	return db, nil
}

// Close closes an DB instance after use.
func (db *DB) Close() {
}

// ComputeBucketID returns the record bucket id by given record id.
func (db *DB) ComputeBucketID(id uint64) uint64 {
	return id/db.redisHashMaxZiplistEntries + 1
}

// GenMaxIDKey generates key of max record id.
func (db *DB) GenMaxIDKey() (maxIDKey string) {
	return fmt.Sprintf("%v/maxid", db.name)
}

// GetMaxID gets max record id.
func (db *DB) GetMaxID(c redis.Conn) (maxID uint64, err error) {
	k := db.GenMaxIDKey()
	exists := false
	if exists, err = redis.Bool(c.Do("EXISTS", k)); err != nil {
		goto end
	}

	if !exists {
		maxID = 0
		goto end
	}

	if maxID, err = redis.Uint64(c.Do("GET", k)); err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("GenMaxId() error: %v\n", err)
		return 0, err
	}

	return maxID, nil
}

// GenMaxBucketIDKey generates the key of max record bucket id.
func (db *DB) GenMaxBucketIDKey() (maxBucketIDKey string) {
	return fmt.Sprintf("%v/maxbucketid", db.name)
}

// GetMaxBucketID gets the max record bucket id.
func (db *DB) GetMaxBucketID(c redis.Conn) (maxBucketID uint64, err error) {
	k := db.GenMaxBucketIDKey()
	exists := false
	if exists, err = redis.Bool(c.Do("EXISTS", k)); err != nil {
		goto end
	}

	if !exists {
		if _, err := c.Do("SET", k, 1); err != nil {
			goto end
		}
	}

	if maxBucketID, err = redis.Uint64(c.Do("GET", k)); err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("GetMaxBucketID() error: %v\n", err)
		return 1, err
	}

	return maxBucketID, nil
}

// GenRecordHashKey generates the record hash(bucket) key by given record id.
func (db *DB) GenRecordHashKey(id uint64) string {
	bucketID := db.ComputeBucketID(id)
	return fmt.Sprintf("%v/bucket/%v", db.name, bucketID)
}

// GenIndexHashKey generates the index hash(bucket) key by given record data.
func (db *DB) GenIndexHashKey(data string) string {
	checkSum := crc32.ChecksumIEEE([]byte(data))
	bucketID := uint64(checkSum) % db.estIndexBucketNum
	return fmt.Sprintf("%v/idx/bucket/%v", db.name, bucketID)
}

// Exists checks if given record exists in database.
func (db *DB) Exists(c redis.Conn, data string) (exists bool, err error) {
	exists = false
	indexHashKey := ""
	indexHashField := data

	indexHashKey = db.GenIndexHashKey(data)
	if exists, err = redis.Bool(c.Do("HEXISTS", indexHashKey, indexHashField)); err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("Exists() error: %v\n", err)
		return false, err
	}

	return exists, nil
}

// Create creates a new record in database.
func (db *DB) Create(c redis.Conn, data string) (id string, err error) {
	ids := []string{}
	if ids, err = db.BatchCreate(c, []string{data}); err != nil {
		goto end
	}

	if len(ids) != 1 {
		err = fmt.Errorf("Count of created record != 1.")
		goto end
	}

end:
	if err != nil {
		debugPrintf("Create() error: %v\n", err)
		return "", err
	}

	return ids[0], nil
}

// BatchCreate creates records in database.
func (db *DB) BatchCreate(c redis.Conn, dataArr []string) (ids []string, err error) {
	var checkedData = make(map[string]int) // key: data, value: order in dataArr.
	var nID, maxID, bucketID, maxBucketID, recordHashField uint64
	var maxBucketIDKey, recordHashKey, indexHashKey, indexHashField, maxIDKey string
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
			err = fmt.Errorf("Empty data.")
			goto end
		}

		// Check redundant data in dataArr.
		if _, ok = checkedData[data]; ok {
			err = fmt.Errorf("Redundant data found in dataArr: %v", data)
			goto end
		}
		checkedData[data] = i

		// Check if data already exist in db.
		if exists, err = db.Exists(c, data); err != nil {
			goto end
		}

		if exists {
			err = fmt.Errorf("Data already exists in db: %v.", data)
			goto end
		}
	}

	// Get max id.
	if maxID, err = db.GetMaxID(c); err != nil {
		goto end
	}

	// Get max id key.
	maxIDKey = db.GenMaxIDKey()

	// Get max bucket id key.
	if maxBucketIDKey = db.GenMaxBucketIDKey(); err != nil {
		goto end
	}

	// Get max bucket id.
	if maxBucketID, err = db.GetMaxBucketID(c); err != nil {
		goto end
	}

	// Prepare piplined transaction.
	c.Send("MULTI")
	alreadySendMULTI = true

	for i, data := range dataArr {
		// Increase Id
		nID = maxID + uint64(i+1)
		id = strconv.FormatUint(nID, 10)
		// Insert to result id array
		ids = append(ids, id)

		// Compute bucket id.
		bucketID = db.ComputeBucketID(nID)

		// Generate hash key for record and index.
		recordHashKey = db.GenRecordHashKey(nID)
		indexHashKey = db.GenIndexHashKey(data)

		// Create record and index.
		recordHashField = nID
		indexHashField = data

		c.Send("HSET", recordHashKey, recordHashField, data)
		c.Send("HSET", indexHashKey, indexHashField, nID)
	}

	// Increase max bucket id if need.
	if bucketID > maxBucketID {
		c.Send("SET", maxBucketIDKey, bucketID)
	}

	c.Send("INCRBY", maxIDKey, len(dataArr))

	// Do piplined transaction.
	if ret, err = c.Do("EXEC"); err != nil {
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

// IDExists checks if record with given record id exists in database.
func (db *DB) IDExists(c redis.Conn, id string) (exists bool, err error) {
	var nID uint64
	var recordHashKey string

	if nID, err = strconv.ParseUint(id, 10, 64); err != nil {
		goto end
	}

	recordHashKey = db.GenRecordHashKey(nID)
	if exists, err = redis.Bool(c.Do("HEXISTS", recordHashKey, nID)); err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("IDExists()  error: %v\n", err)
		return false, err
	}

	return exists, nil
}

// BatchGet returns multiple record data by given record ids.
//
//     Params
//         ids: record id array.
//     Return:
//         records: record array.
func (db *DB) BatchGet(c redis.Conn, ids []string) (records []Record, err error) {
	var nID uint64
	recordHashKey := ""
	alreadySendMULTI := false
	dataArr := []string{}

	if len(ids) == 0 {
		err = fmt.Errorf("Empty id array")
		goto end
	}

	// Prepare piplined transaction.
	c.Send("MULTI")
	alreadySendMULTI = true

	for _, id := range ids {
		if nID, err = strconv.ParseUint(id, 10, 64); err != nil {
			goto end
		}

		recordHashKey = db.GenRecordHashKey(nID)
		c.Send("HGET", recordHashKey, nID)
	}

	// Do piplined transaction.
	if dataArr, err = redis.Strings(c.Do("EXEC")); err != nil {
		goto end
	}

	if len(dataArr) != len(ids) {
		err = fmt.Errorf("returned data array length != id array length")
		goto end
	}

	for i, id := range ids {
		records = append(records, Record{ID: id, Data: dataArr[i]})
	}

	debugPrintf("BatchGet() ok. records: %v\n", records)
end:
	if err != nil {
		if alreadySendMULTI {
			c.Do("DISCARD")
		}
		debugPrintf("BatchGet() error: %v\n", err)
		return []Record{}, err
	}

	return records, nil
}

// Get returns record data by given record id.
func (db *DB) Get(c redis.Conn, id string) (r Record, err error) {
	records := []Record{}

	if records, err = db.BatchGet(c, []string{id}); err != nil {
		goto end
	}
end:
	if err != nil {
		debugPrintf("Get() error: %v\n", err)
		return Record{}, err
	}

	return records[0], nil
}

// Update updates the record by given id and new data.
func (db *DB) Update(c redis.Conn, record Record) error {
	return db.BatchUpdate(c, []Record{record})
}

// BatchUpdate updates multiple records by given ids and new data.
//
//     Params:
//         records: record array to be updated.
func (db *DB) BatchUpdate(c redis.Conn, records []Record) (err error) {
	type updateInfo struct {
		data              string
		recordHashKey     string
		recordHashField   uint64
		oldIndexHashKey   string
		oldIndexHashField string
		newIndexHashKey   string
		newIndexHashField string
	}

	var oldRecord = Record{}
	var recordHashKey, oldIndexHashKey, newIndexHashKey string
	var nID, recordHashField uint64
	var ret interface{}
	exists := false
	updateInfoMap := make(map[uint64]updateInfo)
	alreadySendMULTI := false

	// Check records.
	for _, r := range records {
		if exists, err = db.IDExists(c, r.ID); err != nil {
			goto end
		}

		if !exists {
			err = fmt.Errorf("Id: %v does not exist.", r.ID)
			goto end
		}

		if oldRecord, err = db.Get(c, r.ID); err != nil {
			goto end
		}

		if nID, err = strconv.ParseUint(r.ID, 10, 64); err != nil {
			goto end
		}

		recordHashKey = db.GenRecordHashKey(nID)
		recordHashField = nID
		oldIndexHashKey = db.GenIndexHashKey(oldRecord.Data)
		newIndexHashKey = db.GenIndexHashKey(r.Data)

		// Check if new data equals to old Data: no need to update.
		if !bytes.Equal([]byte(r.Data), []byte(oldRecord.Data)) {
			updateInfoMap[nID] = updateInfo{
				data:              r.Data,
				recordHashKey:     recordHashKey,
				recordHashField:   recordHashField,
				oldIndexHashKey:   oldIndexHashKey,
				oldIndexHashField: oldRecord.Data,
				newIndexHashKey:   newIndexHashKey,
				newIndexHashField: r.Data}
		}

		// Check if data already exists in db(may be with another id).
		if exists, err = db.Exists(c, r.Data); err != nil {
			goto end
		}

		if exists {
			err = fmt.Errorf("Data already exists in db: %v.", r.Data)
			goto end
		}
	}

	// Prepare pipelined transaction.
	c.Send("MULTI")
	alreadySendMULTI = true

	for nID, info := range updateInfoMap {
		c.Send("HSET", info.recordHashKey, info.recordHashField, info.data)
		c.Send("HSET", info.newIndexHashKey, info.newIndexHashField, nID)
		c.Send("HDEL", info.oldIndexHashKey, info.oldIndexHashField)
	}

	if ret, err = c.Do("EXEC"); err != nil {
		goto end
	}

	debugPrintf("BatchUpdate() ok. ret: %v\n", ret)

end:
	if err != nil {
		if alreadySendMULTI {
			c.Do("DISCARD")
		}
		debugPrintf("BatchUpdate() error: %v\n", err)
		return err
	}

	return nil
}

// Delete deletes the record in database by given id.
func (db *DB) Delete(c redis.Conn, id string) (err error) {
	return db.BatchDelete(c, []string{id})
}

// BatchDelete deletes multiple records in database by given ids.
func (db *DB) BatchDelete(c redis.Conn, ids []string) (err error) {
	type delInfo struct {
		recordHashKey   string
		recordHashField uint64
		indexHashKey    string
		indexHashField  string
	}

	var record = Record{}
	var recordHashKey, indexHashKey string
	var nID, recordHashField uint64
	var ret interface{}
	delInfoMap := make(map[uint64]delInfo)
	exists := false
	alreadySendMULTI := false

	// Check Id
	for _, id := range ids {
		if exists, err = db.IDExists(c, id); err != nil {
			goto end
		}

		if !exists {
			err = fmt.Errorf("id:%v does not exist", id)
			goto end
		}

		if record, err = db.Get(c, id); err != nil {
			goto end
		}

		if nID, err = strconv.ParseUint(id, 10, 64); err != nil {
			goto end
		}

		recordHashKey = db.GenRecordHashKey(nID)
		recordHashField = nID
		indexHashKey = db.GenIndexHashKey(record.Data)

		delInfoMap[recordHashField] = delInfo{
			recordHashKey:   recordHashKey,
			recordHashField: recordHashField,
			indexHashKey:    indexHashKey,
			indexHashField:  record.Data,
		}
	}

	// Prepare pipelined transaction.
	c.Send("MULTI")
	alreadySendMULTI = true

	for _, info := range delInfoMap {
		c.Send("HDEL", info.recordHashKey, info.recordHashField)
		c.Send("HDEL", info.indexHashKey, info.indexHashField)
	}

	if ret, err = c.Do("EXEC"); err != nil {
		goto end
	}

	debugPrintf("BatchDelete() ok. ret: %v\n", ret)

end:
	if err != nil {
		if alreadySendMULTI {
			c.Do("DISCARD")
		}
		debugPrintf("BatchDelete() error: %v\n", err)
		return err
	}

	return nil

}

// Search scans all indexes(record data) in database and use the pattern of Redis "SCAN" command to find records which match the pattern.
//
//     Params:
//         pattern: pattern of Redis "SCAN" command.
//         It'll return all record ID if pattern is empty.
//         Ex: `{"name":"Frank*"}*`
//     Returns:
//         ids: matched record ids.
func (db *DB) Search(c redis.Conn, pattern string) (ids []string, err error) {
	var cursor, subCursor uint64
	var l int
	var v []interface{}
	keys := []string{}
	items := []string{}
	ids = []string{}

	cursor = 0
	for {
		if v, err = redis.Values(c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024)); err != nil {
			goto end
		}

		if v, err = redis.Scan(v, &cursor, &keys); err != nil {
			goto end
		}

		for _, k := range keys {
			subCursor = 0
			for {
				if len(pattern) != 0 {
					if v, err = redis.Values(c.Do("HSCAN", k, subCursor, "match", pattern, "COUNT", 1024)); err != nil {
						goto end
					}
				} else {
					if v, err = redis.Values(c.Do("HSCAN", k, subCursor, "COUNT", 1024)); err != nil {
						goto end
					}

				}

				if v, err = redis.Scan(v, &subCursor, &items); err != nil {
					goto end
				}

				l = len(items)
				if l > 0 {
					if l%2 != 0 {
						err = fmt.Errorf("Search() error: HSCAN result error.")
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
		debugPrintf("Search() error: %v\n", err)
		return []string{}, err
	}

	return ids, nil
}

// RegexpSearch scans all indexes(record data) in database and use regexp patterns to find records which match the patterns.
//
//     Params:
//         patterns: regexp pattern array. Ex: {`{"name":"Frank.+"}`,`{"tel":"136\d{8}"}`}
//     Returns:
//         ids: matched record ids map. key: pattern, value: matched record ids.
func (db *DB) RegexpSearch(c redis.Conn, patterns []string) (ids [][]string, err error) {
	var cursor, subCursor uint64
	var l int
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
		if v, err = redis.Values(c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024)); err != nil {
			goto end
		}

		if v, err = redis.Scan(v, &cursor, &keys); err != nil {
			goto end
		}

		for _, k := range keys {
			subCursor = 0
			for {
				if v, err = redis.Values(c.Do("HSCAN", k, subCursor, "COUNT", 1024)); err != nil {
					goto end
				}

				if v, err = redis.Scan(v, &subCursor, &items); err != nil {
					goto end
				}

				l = len(items)
				if l > 0 {
					if l%2 != 0 {
						err = fmt.Errorf("Search() error: HSCAN result error.")
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
		debugPrintf("Search() error: %v\n", err)
		return [][]string{}, err
	}

	return ids, nil
}

// Count returns record count stored in Redis.
func (db *DB) Count(c redis.Conn) (count uint64, err error) {
	var maxBucketID, n uint64

	if maxBucketID, err = db.GetMaxBucketID(c); err != nil {
		goto end
	}

	for i := maxBucketID; i >= 1; i-- {
		recordHashKey := fmt.Sprintf("%v/bucket/%v", db.name, i)
		if n, err = redis.Uint64(c.Do("HLEN", recordHashKey)); err != nil {
			goto end
		}

		if n > 0 {
			count += n
		}
	}
end:
	if err != nil {
		debugPrintf("Count() error: %v\n", err)
		return 0, err
	}

	return count, nil
}

// Info returns the information of current DB.
//
//     Returns:
//         infoMap: key: section, value: information.
func (db *DB) Info(c redis.Conn) (infoMap map[string]string, err error) {
	var maxID, maxBucketID, recordBucketNum, recordNum, indexBucketNum, indexNum, n, cursor uint64
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

	if maxID, err = db.GetMaxID(c); err != nil {
		goto end
	}
	infoMap["max id"] = strconv.FormatUint(maxID, 10)

	if maxBucketID, err = db.GetMaxBucketID(c); err != nil {
		goto end
	}
	infoMap["max bucket id"] = strconv.FormatUint(maxBucketID, 10)

	// Check record hashes' encoding
	for i := maxBucketID; i >= 1; i-- {
		recordHashKey = fmt.Sprintf("%v/bucket/%v", db.name, i)
		if n, err = redis.Uint64(c.Do("HLEN", recordHashKey)); err != nil {
			goto end
		}

		if n > 0 {
			recordBucketNum++
			recordNum += n

			// Check hash encoding: ziplist or hashtable.
			if ret, err = redis.String(c.Do("DEBUG", "OBJECT", recordHashKey)); err != nil {
				debugPrintf("failed to exec debug object, %v\n.", recordHashKey)
				goto end
			}

			encoding = re.FindString(ret)
			if encoding == "" {
				err = fmt.Errorf("Can not find encoding of %v.", recordHashKey)
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
		if v, err = redis.Values(c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024)); err != nil {
			goto end
		}

		if v, err = redis.Scan(v, &cursor, &keys); err != nil {
			goto end
		}

		for _, k := range keys {
			indexBucketNum++

			if n, err = redis.Uint64(c.Do("HLEN", k)); err != nil {
				goto end
			}
			indexNum += n

			// Check hash encoding: ziplist or hashtable
			if ret, err = redis.String(c.Do("DEBUG", "OBJECT", k)); err != nil {
				goto end
			}

			encoding = re.FindString(ret)
			if encoding == "" {
				err = fmt.Errorf("Can not find encoding of %v.", recordHashKey)
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

	infoMap["db.name"] = db.name
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
		debugPrintf("Info() error: %v\n", err)
		return make(map[string]string), err
	}
	return infoMap, nil
}
