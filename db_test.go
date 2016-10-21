package simpledb_test

import (
	"log"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/northbright/simpledb"
)

func ExampleDB_Create() {
	var err error
	var db *simpledb.DB
	id := ""
	data := `{"name":"Bob Smith","tel":"13500135000"}`

	log.Printf("\n")
	log.Printf("--------- Create() Test Begin --------\n")

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	id, err = db.Create(c, data)
	if err != nil {
		goto end
	}

	data, err = db.Get(c, id)
	if err != nil {
		goto end
	}

	log.Printf("Result: id: %v, data: %v\n", id, data)
end:
	log.Printf("--------- Create() Test End --------\n")
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

	log.Printf("\n")
	log.Printf("--------- BatchCreate() Test Begin --------\n")

	dataMap = make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	ids, err = db.BatchCreate(c, data)
	if err != nil {
		goto end
	}

	log.Printf("Result:\n")
	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	for id, data := range dataMap {
		log.Printf("id: %v, data: %v\n", id, data)
	}

end:
	log.Printf("--------- BatchCreate() Test End --------\n")
	// Output:
}

func ExampleDB_IDExists() {
	var err error
	var db *simpledb.DB
	exists := false
	id := "1"

	log.Printf("\n")
	log.Printf("--------- IDExists() Test Begin --------\n")

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	exists, err = db.IDExists(c, id)
	if err != nil {
		goto end
	}

	log.Printf("IdExsits(%v): %v\n", id, exists)

end:
	log.Printf("--------- IDExists() Test End --------\n")
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

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Get() Test Begin --------\n")
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

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", ids[0], data)

end:
	log.Printf("--------- Get() Test End --------\n")
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

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- BatchGet() Test Begin --------\n")
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}
	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	for id, data := range dataMap {
		log.Printf("id: %v, data: %v\n", id, data)
	}

end:
	log.Printf("--------- BatchGet() Test End --------\n")
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

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Update() Test Begin --------\n")
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

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", ids[0], data)

	// Change phone number from 135xxx to 138xxx.
	data = strings.Replace(data, "135", "158", -1)
	err = db.Update(c, ids[0], data)
	if err != nil {
		goto end
	}

	log.Printf("Change phone number from 135xxx to 158xxx.")
	data, err = db.Get(c, ids[0])
	if err != nil {
		goto end
	}
	log.Printf("Updated. id: %v, data: %v\n", ids[0], data)

end:
	log.Printf("--------- Update() Test End --------\n")
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

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- BatchUpdate() Test Begin --------\n")
	log.Printf("Search pattern: %v, Result:\n", pattern)
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}
	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	log.Printf("Result:\n")
	for id, data := range dataMap {
		log.Printf("id: %v, data: %v\n", id, data)
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

	log.Printf("Batch update phone number from 138xxx to 186xxx.")
	for id, data := range dataMap {
		log.Printf("id: %v, data: %v\n", id, data)
	}
end:
	log.Printf("--------- BatchUpdate() Test End --------\n")
	// Output:
}

func ExampleDB_Search() {
	var err error
	var db *simpledb.DB
	ids := []string{}
	patterns := []string{
		// Search UTF8 string.
		`*"name":"王*宝"*`,
		// Search name matches Frank* and tel matches 13700137000.
		`*"name":"Frank*"*"tel":"13700137000"*`,
	}

	dataMap := make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Search() Test Begin --------\n")

	for _, p := range patterns {
		log.Printf("Search pattern: %v\n", p)
		ids, err = db.Search(c, p)
		if err != nil {
			goto end
		}

		dataMap, err = db.BatchGet(c, ids)
		if err != nil {
			goto end
		}

		log.Printf("Result:\n")
		for k, v := range dataMap {
			log.Printf("id: %v, data: %v\n", k, v)
		}
	}

end:
	log.Printf("--------- Search() Test End --------\n")
	// Output:
}

func ExampleDB_RegexpSearch() {
	var err error
	var db *simpledb.DB
	ids := [][]string{}
	patterns := []string{
		// Search UTF8 string.
		//`"name":"王.*宝"`,
		`王.+宝`,
		// Search name matches Frank* and tel matches 13700137000.
		`"Frank.*".*"tel":"13700137000"`,
	}

	dataMap := make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- RegexpSearch() Test Begin --------\n")

	ids, err = db.RegexpSearch(c, patterns)
	if err != nil {
		goto end
	}

	for i, p := range patterns {
		log.Printf("Regexp pattern: %v\n", p)
		dataMap, err = db.BatchGet(c, ids[i])
		if err != nil {
			goto end
		}

		log.Printf("Result:\n")
		for k, v := range dataMap {
			log.Printf("id: %v, data: %v\n", k, v)
		}
	}
end:
	log.Printf("--------- RegexpSearch() Test End --------\n")
	// Output:
}

func ExampleDB_Info() {
	var err error
	var db *simpledb.DB
	infoMap := make(map[string]string)

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Info() Test Begin --------\n")

	infoMap, err = db.Info(c)
	if err != nil {
		goto end
	}
	log.Printf("Info():\n")
	for k, v := range infoMap {
		log.Printf("%v: %v\n", k, v)
	}

end:
	log.Printf("--------- Info() Test End --------\n")
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

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Delete() Test Begin --------\n")

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

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", ids[0], data)

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

	log.Printf("Search again after record deleted. Result:\n")
	log.Printf("ids: %v\n", ids)

end:
	log.Printf("--------- Delete() Test End --------\n")
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

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- BatchDelete() Test Begin --------\n")
	// Get all records.
	ids, err = db.Search(c, pattern)
	if err != nil {
		goto end
	}

	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	for id, data := range dataMap {
		log.Printf("id: %v, data: %v\n", id, data)
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

	log.Printf("Search again after record deleted. Result:\n")
	log.Printf("ids: %v\n", ids)

end:
	log.Printf("--------- BatchDelete() Test End --------\n")
	// Output:
}
