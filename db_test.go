package simpledb_test

import (
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/northbright/simpledb"
)

func ExampleDB_Create() {
	var err error
	var db *simpledb.DB
	id := ""
	data := `{"name":"Bob Smith","tel":"13500135000"}`

	simpledb.DebugPrintf("--------- Create() Test Begin --------\n")

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	id, err = db.Create(c, data)
	if err != nil {
		goto end
	}

	data, err = db.Get(c, id)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Result: id: %v, data: %v\n", id, data)
end:
	simpledb.DebugPrintf("--------- Create() Test End --------\n")
	// Output:
}

func ExampleDB_BatchCreate() {
	var err error
	var db *simpledb.DB
	dataMap := make(map[string]string)
	ids := []string{}
	data := []string{
		`{"name":"Frank Xu","tel":"13700137000"}`,
		`{"name":"Frank Wang","tel":"13600136000"}`,
		`{"name":"张三","tel":"13800138001"}`,
		`{"name":"李四","tel":"13800138002"}`,
		`{"name":"王大宝","tel":"13800138003"}`,
		`{"name":"王小宝","tel":"13800138003"}`,
		`{"name":"王宝多","tel":"13700137077"}`,
	}

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- BatchCreate() Test Begin --------\n")

	dataMap = make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	ids, err = db.BatchCreate(c, data)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Result:\n")
	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	for id, data := range dataMap {
		simpledb.DebugPrintf("id: %v, data: %v\n", id, data)
	}

end:
	simpledb.DebugPrintf("--------- BatchCreate() Test End --------\n")
	// Output:
}

func ExampleDB_IdExists() {
	var err error
	var db *simpledb.DB
	exists := false
	var recordHashKey, indexHashKey string
	var recordHashField uint64
	id := "1"

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- IdExists() Test Begin --------\n")

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	exists, recordHashKey, indexHashKey, recordHashField, err = db.IdExists(c, id)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("IdExsits(%v) Result:\n", id)
	simpledb.DebugPrintf("exists: %v, recordHashKey: %v, indexHashKey: %v, recordHashField: %v\n", exists, recordHashKey, indexHashKey, recordHashField)

end:
	simpledb.DebugPrintf("--------- IdExists() Test End --------\n")
	// Output:
}

func ExampleDB_Get() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"Bob*"*`
	data := ""

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- Get() Test Begin --------\n")
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	data, err = db.Get(c, ids[0])
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Search pattern: %v, Result:\n", pattern)
	simpledb.DebugPrintf("id: %v, data: %v\n", ids[0], data)

end:
	simpledb.DebugPrintf("--------- Get() Test End --------\n")
	// Output:
}

func ExampleDB_BatchGet() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"王*"*`
	dataMap := make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- BatchGet() Test Begin --------\n")
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}
	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Search pattern: %v, Result:\n", pattern)
	for id, data := range dataMap {
		simpledb.DebugPrintf("id: %v, data: %v\n", id, data)
	}

end:
	simpledb.DebugPrintf("--------- BatchGet() Test End --------\n")
	// Output:
}

func ExampleDB_Update() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"Bob*"*`
	data := ""

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- Update() Test Begin --------\n")
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	data, err = db.Get(c, ids[0])
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Search pattern: %v, Result:\n", pattern)
	simpledb.DebugPrintf("id: %v, data: %v\n", ids[0], data)

	// Change phone number from 135xxx to 138xxx.
	data = strings.Replace(data, "135", "158", -1)
	err = db.Update(c, ids[0], data)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Change phone number from 135xxx to 158xxx.")
	data, err = db.Get(c, ids[0])
	if err != nil {
		goto end
	}
	simpledb.DebugPrintf("Updated. id: %v, data: %v\n", ids[0], data)

end:
	simpledb.DebugPrintf("--------- Update() Test End --------\n")
	// Output:
}

func ExampleDB_BatchUpdate() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"tel":"138*"*`
	dataMap := make(map[string]string)
	newDataMap := make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- BatchUpdate() Test Begin --------\n")
	simpledb.DebugPrintf("Search pattern: %v, Result:\n", pattern)
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}
	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Result:\n")
	for id, data := range dataMap {
		simpledb.DebugPrintf("id: %v, data: %v\n", id, data)
		newDataMap[id] = strings.Replace(data, "138", "186", -1)
	}

	err = db.BatchUpdate(c, newDataMap)
	if err != nil {
		goto end
	}

	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Batch update phone number from 138xxx to 186xxx.")
	for id, data := range dataMap {
		simpledb.DebugPrintf("id: %v, data: %v\n", id, data)
	}
end:
	simpledb.DebugPrintf("--------- BatchUpdate() Test End --------\n")
	// Output:
}

func ExampleDB_Search() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	patterns := []string{
		// Search UTF8 string.
		`*"name":"王*宝"*`,
		// Search name matches Frank* and tel matches 13800138000.
		`*"name":"Frank*"*"tel":"13700137000"*`,
	}

	dataMap := make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- Search() Test Begin --------\n")

	for _, p := range patterns {
		simpledb.DebugPrintf("Search pattern: %v\n", p)
		ids, err = db.Search(c, p)
		if err != nil {
			goto end
		}

		dataMap, err = db.BatchGet(c, ids)
		if err != nil {
			goto end
		}

		simpledb.DebugPrintf("Result:\n")
		for k, v := range dataMap {
			simpledb.DebugPrintf("id: %v, data: %v\n", k, v)
		}
	}

end:
	simpledb.DebugPrintf("--------- Search() Test End --------\n")
	// Output:
}

func ExampleDB_Delete() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"Frank Wang"*`
	data := ""

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- Delete() Test Begin --------\n")

	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	data, err = db.Get(c, ids[0])
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Search pattern: %v, Result:\n", pattern)
	simpledb.DebugPrintf("id: %v, data: %v\n", ids[0], data)

	// Delete
	err = db.Delete(c, ids[0])
	if err != nil {
		goto end
	}

	// Search again after record deleted.
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Search again after record deleted. Result:\n")
	simpledb.DebugPrintf("ids: %v\n", ids)

end:
	simpledb.DebugPrintf("--------- Delete() Test End --------\n")
	// Output:
}

func ExampleDB_BatchDelete() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"*"*`
	dataMap := make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db = simpledb.Open("student")

	simpledb.DebugPrintf("\n")
	simpledb.DebugPrintf("--------- BatchDelete() Test Begin --------\n")
	// Get all records.
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}

	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Search pattern: %v, Result:\n", pattern)
	for id, data := range dataMap {
		simpledb.DebugPrintf("id: %v, data: %v\n", id, data)
	}

	// Delete all record.
	err = db.BatchDelete(c, ids)
	if err != nil {
		goto end
	}

	// Search again after record deleted.
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}

	simpledb.DebugPrintf("Search again after record deleted. Result:\n")
	simpledb.DebugPrintf("ids: %v\n", ids)

end:
	simpledb.DebugPrintf("--------- BatchDelete() Test End --------\n")
	// Output:
}
