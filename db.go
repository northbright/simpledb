package simpledb

import (
	"fmt"
	"hash/crc32"
	"regexp"
	"strconv"

	"github.com/gomodule/redigo/redis"
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
	// redisAddr is Redis address. Ex: ":6379", "192.168.1.18:6379".
	redisAddr string
	// redisPassword is Redis password.
	redisPassword string
	// c is Redis connection.
	c redis.Conn
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

// genRedisHashMaxZiplistEntriesKey Generates the "hash-max-ziplist-entries" key for DB.
func (db *DB) genRedisHashMaxZiplistEntriesKey() (redisHashMaxZiplistEntriesKey string) {
	return fmt.Sprintf("%v/redis-hash-max-ziplist-entries", db.name)
}

// Open returns an DB instance by given database name.
func Open(redisAddr, redisPassword, name string) (db *DB, err error) {
	db = &DB{redisAddr: redisAddr, redisPassword: redisPassword, name: name}
	exists := false
	k := db.genRedisHashMaxZiplistEntriesKey()

	if db.c, err = GetRedisConn(redisAddr, redisPassword); err != nil {
		goto end
	}

	if len(name) == 0 {
		err = fmt.Errorf("Empty db name.")
		goto end
	}

	if exists, err = redis.Bool(db.c.Do("EXISTS", k)); err != nil {
		goto end
	}

	// Set db.redisHashMaxZiplistEntries only once at first time.
	if !exists {
		if db.redisHashMaxZiplistEntries, err = GetRedisHashMaxZiplistEntries(db.c); err != nil {
			goto end
		}

		if _, err = db.c.Do("SET", k, db.redisHashMaxZiplistEntries); err != nil {
			goto end
		}
	} else {
		if db.redisHashMaxZiplistEntries, err = redis.Uint64(db.c.Do("GET", k)); err != nil {
			goto end
		}
	}

	// Initialize estimated index bucket number.
	db.estIndexBucketNum = EstimatedMaxRecordNum / uint64(float64(db.redisHashMaxZiplistEntries)*0.9)
	// Initialize index hash key scan pattern.
	db.indexHashKeyScanPattern = fmt.Sprintf("%v/idx/bucket/*", db.name)
end:
	if err != nil {
		debugPrintf("Open(%v) error: %v\n", name, err)
		return nil, err
	}

	return db, nil
}

// Close closes an DB instance after use.
func (db *DB) Close() {
	db.c.Close()
}

// computeBucketID returns the record bucket id by given record id.
func (db *DB) computeBucketID(id uint64) uint64 {
	return id/db.redisHashMaxZiplistEntries + 1
}

// genMaxIDKey generates key of max record id.
func (db *DB) genMaxIDKey() (maxIDKey string) {
	return fmt.Sprintf("%v/maxid", db.name)
}

// GetMaxID gets max record id.
func (db *DB) GetMaxID() (maxID uint64, err error) {
	k := db.genMaxIDKey()
	exists := false
	if exists, err = redis.Bool(db.c.Do("EXISTS", k)); err != nil {
		goto end
	}

	if !exists {
		maxID = 0
		goto end
	}

	if maxID, err = redis.Uint64(db.c.Do("GET", k)); err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("GenMaxId() error: %v\n", err)
		return 0, err
	}

	return maxID, nil
}

// genMaxBucketIDKey generates the key of max record bucket id.
func (db *DB) genMaxBucketIDKey() (maxBucketIDKey string) {
	return fmt.Sprintf("%v/maxbucketid", db.name)
}

// GetMaxBucketID gets the max record bucket id.
func (db *DB) GetMaxBucketID() (maxBucketID uint64, err error) {
	k := db.genMaxBucketIDKey()
	exists := false
	if exists, err = redis.Bool(db.c.Do("EXISTS", k)); err != nil {
		goto end
	}

	if !exists {
		if _, err := db.c.Do("SET", k, 1); err != nil {
			goto end
		}
	}

	if maxBucketID, err = redis.Uint64(db.c.Do("GET", k)); err != nil {
		goto end
	}

end:
	if err != nil {
		debugPrintf("GetMaxBucketID() error: %v\n", err)
		return 1, err
	}

	return maxBucketID, nil
}

// genRecordHashKey generates the record hash(bucket) key by given record id.
func (db *DB) genRecordHashKey(id uint64) string {
	bucketID := db.computeBucketID(id)
	return fmt.Sprintf("%v/bucket/%v", db.name, bucketID)
}

// genIndexHashKey generates the index hash(bucket) key by given record data.
func (db *DB) genIndexHashKey(data string) string {
	checkSum := crc32.ChecksumIEEE([]byte(data))
	bucketID := uint64(checkSum) % db.estIndexBucketNum
	return fmt.Sprintf("%v/idx/bucket/%v", db.name, bucketID)
}

// Exists checks if given record exists in database.
func (db *DB) Exists(data string) (exists bool, err error) {
	exists = false
	indexHashKey := ""
	indexHashField := data

	indexHashKey = db.genIndexHashKey(data)
	if exists, err = redis.Bool(db.c.Do("HEXISTS", indexHashKey, indexHashField)); err != nil {
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
func (db *DB) Create(data string) (id string, err error) {
	ids := []string{}
	if ids, err = db.BatchCreate([]string{data}); err != nil {
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
func (db *DB) BatchCreate(dataArr []string) (ids []string, err error) {
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
		if exists, err = db.Exists(data); err != nil {
			goto end
		}

		if exists {
			err = fmt.Errorf("Data already exists in db: %v.", data)
			goto end
		}
	}

	// Get max id.
	if maxID, err = db.GetMaxID(); err != nil {
		goto end
	}

	// Get max id key.
	maxIDKey = db.genMaxIDKey()

	// Get max bucket id key.
	if maxBucketIDKey = db.genMaxBucketIDKey(); err != nil {
		goto end
	}

	// Get max bucket id.
	if maxBucketID, err = db.GetMaxBucketID(); err != nil {
		goto end
	}

	// Prepare piplined transaction.
	db.c.Send("MULTI")
	alreadySendMULTI = true

	for i, data := range dataArr {
		// Increase Id
		nID = maxID + uint64(i+1)
		id = strconv.FormatUint(nID, 10)
		// Insert to result id array
		ids = append(ids, id)

		// Compute bucket id.
		bucketID = db.computeBucketID(nID)

		// Generate hash key for record and index.
		recordHashKey = db.genRecordHashKey(nID)
		indexHashKey = db.genIndexHashKey(data)

		// Create record and index.
		recordHashField = nID
		indexHashField = data

		db.c.Send("HSET", recordHashKey, recordHashField, data)
		db.c.Send("HSET", indexHashKey, indexHashField, nID)
	}

	// Increase max bucket id if need.
	if bucketID > maxBucketID {
		db.c.Send("SET", maxBucketIDKey, bucketID)
	}

	db.c.Send("INCRBY", maxIDKey, len(dataArr))

	// Do piplined transaction.
	if ret, err = db.c.Do("EXEC"); err != nil {
		goto end
	}

	debugPrintf("BatchCreate() ok. ret: %v, ids: %v\n", ret, ids)

end:
	if err != nil {
		if alreadySendMULTI {
			db.c.Do("DISCARD")
		}
		debugPrintf("BatchCreate() error: %v\n", err)
		return []string{}, err
	}

	return ids, nil
}

// IDExists checks if record with given record id exists in database.
func (db *DB) IDExists(id string) (exists bool, err error) {
	var nID uint64
	var recordHashKey string

	if nID, err = strconv.ParseUint(id, 10, 64); err != nil {
		goto end
	}

	recordHashKey = db.genRecordHashKey(nID)
	if exists, err = redis.Bool(db.c.Do("HEXISTS", recordHashKey, nID)); err != nil {
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
func (db *DB) BatchGet(ids []string) (records []Record, err error) {
	var nID uint64
	recordHashKey := ""
	alreadySendMULTI := false
	dataArr := []string{}

	if len(ids) == 0 {
		err = fmt.Errorf("Empty id array")
		goto end
	}

	// Prepare piplined transaction.
	db.c.Send("MULTI")
	alreadySendMULTI = true

	for _, id := range ids {
		if nID, err = strconv.ParseUint(id, 10, 64); err != nil {
			goto end
		}

		recordHashKey = db.genRecordHashKey(nID)
		db.c.Send("HGET", recordHashKey, nID)
	}

	// Do piplined transaction.
	if dataArr, err = redis.Strings(db.c.Do("EXEC")); err != nil {
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
			db.c.Do("DISCARD")
		}
		debugPrintf("BatchGet() error: %v\n", err)
		return []Record{}, err
	}

	return records, nil
}

// Get returns record data by given record id.
func (db *DB) Get(id string) (r Record, err error) {
	records := []Record{}

	if records, err = db.BatchGet([]string{id}); err != nil {
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
func (db *DB) Update(record Record) error {
	return db.BatchUpdate([]Record{record})
}

// BatchUpdate updates multiple records by given ids and new data.
//
//     Params:
//         records: record array to be updated.
func (db *DB) BatchUpdate(records []Record) (err error) {
	type updateInfo struct {
		recordHashKey     string
		recordHashField   uint64
		recordHashValue   string
		oldIndexHashKey   string
		oldIndexHashField string
		newIndexHashKey   string
		newIndexHashField string
		newIndexHashValue uint64
	}

	var oldRecord = Record{}
	var nID uint64
	var ret interface{}
	exists := false
	updateInfos := []updateInfo{}
	alreadySendMULTI := false

	// Check records.
	for _, r := range records {
		if exists, err = db.IDExists(r.ID); err != nil {
			goto end
		}

		if !exists {
			err = fmt.Errorf("Id: %v does not exist.", r.ID)
			goto end
		}

		// Check if data already exists in db(may be with another id).
		if exists, err = db.Exists(r.Data); err != nil {
			err = fmt.Errorf("Data already exists in db: %v.", r.Data)
			goto end
		}

		if oldRecord, err = db.Get(r.ID); err != nil {
			goto end
		}

		if nID, err = strconv.ParseUint(r.ID, 10, 64); err != nil {
			goto end
		}

		info := updateInfo{
			recordHashKey:     db.genRecordHashKey(nID),
			recordHashField:   nID,
			recordHashValue:   r.Data,
			oldIndexHashKey:   db.genIndexHashKey(oldRecord.Data),
			oldIndexHashField: oldRecord.Data,
			newIndexHashKey:   db.genIndexHashKey(r.Data),
			newIndexHashField: r.Data,
			newIndexHashValue: nID,
		}
		updateInfos = append(updateInfos, info)
	}

	// Prepare pipelined transaction.
	db.c.Send("MULTI")
	alreadySendMULTI = true

	for _, info := range updateInfos {
		db.c.Send("HSET", info.recordHashKey, info.recordHashField, info.recordHashValue)
		db.c.Send("HSET", info.newIndexHashKey, info.newIndexHashField, info.newIndexHashValue)
		db.c.Send("HDEL", info.oldIndexHashKey, info.oldIndexHashField)
	}

	if ret, err = db.c.Do("EXEC"); err != nil {
		goto end
	}

	debugPrintf("BatchUpdate() ok. ret: %v\n", ret)

end:
	if err != nil {
		if alreadySendMULTI {
			db.c.Do("DISCARD")
		}
		debugPrintf("BatchUpdate() error: %v\n", err)
		return err
	}

	return nil
}

// Delete deletes the record in database by given id.
func (db *DB) Delete(id string) (err error) {
	return db.BatchDelete([]string{id})
}

// BatchDelete deletes multiple records in database by given ids.
func (db *DB) BatchDelete(ids []string) (err error) {
	type delInfo struct {
		recordHashKey   string
		recordHashField uint64
		indexHashKey    string
		indexHashField  string
	}

	var record = Record{}
	var nID uint64
	var ret interface{}
	delInfos := []delInfo{}
	exists := false
	alreadySendMULTI := false

	// Check Id
	for _, id := range ids {
		if exists, err = db.IDExists(id); err != nil {
			goto end
		}

		if !exists {
			err = fmt.Errorf("id:%v does not exist", id)
			goto end
		}

		if record, err = db.Get(id); err != nil {
			goto end
		}

		if nID, err = strconv.ParseUint(id, 10, 64); err != nil {
			goto end
		}

		info := delInfo{
			recordHashKey:   db.genRecordHashKey(nID),
			recordHashField: nID,
			indexHashKey:    db.genIndexHashKey(record.Data),
			indexHashField:  record.Data,
		}
		delInfos = append(delInfos, info)
	}

	// Prepare pipelined transaction.
	db.c.Send("MULTI")
	alreadySendMULTI = true

	for _, info := range delInfos {
		db.c.Send("HDEL", info.recordHashKey, info.recordHashField)
		db.c.Send("HDEL", info.indexHashKey, info.indexHashField)
	}

	if ret, err = db.c.Do("EXEC"); err != nil {
		goto end
	}

	debugPrintf("BatchDelete() ok. ret: %v\n", ret)

end:
	if err != nil {
		if alreadySendMULTI {
			db.c.Do("DISCARD")
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
func (db *DB) Search(pattern string) (ids []string, err error) {
	var cursor, subCursor uint64
	var l int
	var v []interface{}
	keys := []string{}
	items := []string{}
	ids = []string{}

	cursor = 0
	for {
		if v, err = redis.Values(db.c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024)); err != nil {
			goto end
		}

		if v, err = redis.Scan(v, &cursor, &keys); err != nil {
			goto end
		}

		for _, k := range keys {
			subCursor = 0
			for {
				if len(pattern) != 0 {
					if v, err = redis.Values(db.c.Do("HSCAN", k, subCursor, "match", pattern, "COUNT", 1024)); err != nil {
						goto end
					}
				} else {
					if v, err = redis.Values(db.c.Do("HSCAN", k, subCursor, "COUNT", 1024)); err != nil {
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
//         ids: matched record ids arrays. Each array contains result IDs match the pattern.
func (db *DB) RegexpSearch(patterns []string) (ids [][]string, err error) {
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
		if v, err = redis.Values(db.c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024)); err != nil {
			goto end
		}

		if v, err = redis.Scan(v, &cursor, &keys); err != nil {
			goto end
		}

		for _, k := range keys {
			subCursor = 0
			for {
				if v, err = redis.Values(db.c.Do("HSCAN", k, subCursor, "COUNT", 1024)); err != nil {
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
func (db *DB) Count() (count uint64, err error) {
	var maxBucketID, n uint64

	if maxBucketID, err = db.GetMaxBucketID(); err != nil {
		goto end
	}

	for i := maxBucketID; i >= 1; i-- {
		recordHashKey := fmt.Sprintf("%v/bucket/%v", db.name, i)
		if n, err = redis.Uint64(db.c.Do("HLEN", recordHashKey)); err != nil {
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
func (db *DB) Info() (infoMap map[string]string, err error) {
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

	if maxID, err = db.GetMaxID(); err != nil {
		goto end
	}
	infoMap["max id"] = strconv.FormatUint(maxID, 10)

	if maxBucketID, err = db.GetMaxBucketID(); err != nil {
		goto end
	}
	infoMap["max bucket id"] = strconv.FormatUint(maxBucketID, 10)

	// Check record hashes' encoding
	for i := maxBucketID; i >= 1; i-- {
		recordHashKey = fmt.Sprintf("%v/bucket/%v", db.name, i)
		if n, err = redis.Uint64(db.c.Do("HLEN", recordHashKey)); err != nil {
			goto end
		}

		if n > 0 {
			recordBucketNum++
			recordNum += n

			// Check hash encoding: ziplist or hashtable.
			if ret, err = redis.String(db.c.Do("DEBUG", "OBJECT", recordHashKey)); err != nil {
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
		if v, err = redis.Values(db.c.Do("SCAN", cursor, "match", db.indexHashKeyScanPattern, "COUNT", 1024)); err != nil {
			goto end
		}

		if v, err = redis.Scan(v, &cursor, &keys); err != nil {
			goto end
		}

		for _, k := range keys {
			indexBucketNum++

			if n, err = redis.Uint64(db.c.Do("HLEN", k)); err != nil {
				goto end
			}
			indexNum += n

			// Check hash encoding: ziplist or hashtable
			if ret, err = redis.String(db.c.Do("DEBUG", "OBJECT", k)); err != nil {
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
