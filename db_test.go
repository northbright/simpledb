package simpledb_test

import (
	"log"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/northbright/simpledb"
)

func ExampleDB_Create() {
	var err error
	var c redis.Conn
	var db *simpledb.DB
	id := ""
	data := `{"name":"Bob Smith","tel":"13500135000"}`

	log.Printf("\n")
	log.Printf("--------- Create() Test Begin --------\n")

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	if id, err = db.Create(c, data); err != nil {
		goto end
	}

	if data, err = db.Get(c, id); err != nil {
		goto end
	}

	log.Printf("Result: id: %v, data: %v\n", id, data)
end:
	log.Printf("--------- Create() Test End --------\n")
	// Output:
}

func ExampleDB_BatchCreate() {
	var err error
	var c redis.Conn
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

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	if ids, err = db.BatchCreate(c, data); err != nil {
		goto end
	}

	log.Printf("Result:\n")
	if dataMap, err = db.BatchGet(c, ids); err != nil {
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
	var c redis.Conn
	var db *simpledb.DB
	exists := false
	id := "1"

	log.Printf("\n")
	log.Printf("--------- IDExists() Test Begin --------\n")

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	if exists, err = db.IDExists(c, id); err != nil {
		goto end
	}

	log.Printf("IdExsits(%v): %v\n", id, exists)

end:
	log.Printf("--------- IDExists() Test End --------\n")
	// Output:
}

func ExampleDB_Get() {
	var err error
	var c redis.Conn
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"Bob*"*`
	data := ""

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Get() Test Begin --------\n")
	if ids, err = db.Search(c, pattern); err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	if data, err = db.Get(c, ids[0]); err != nil {
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
	var c redis.Conn
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"王*"*`
	dataMap := make(map[string]string)

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- BatchGet() Test Begin --------\n")
	if ids, err = db.Search(c, pattern); err != nil {
		goto end
	}
	if dataMap, err = db.BatchGet(c, ids); err != nil {
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
	var c redis.Conn
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"Bob*"*`
	data := ""

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Update() Test Begin --------\n")
	if ids, err = db.Search(c, pattern); err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	if data, err = db.Get(c, ids[0]); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", ids[0], data)

	// Change phone number from 135xxx to 138xxx.
	data = strings.Replace(data, "135", "158", -1)
	if err = db.Update(c, ids[0], data); err != nil {
		goto end
	}

	log.Printf("Change phone number from 135xxx to 158xxx.")
	if data, err = db.Get(c, ids[0]); err != nil {
		goto end
	}
	log.Printf("Updated. id: %v, data: %v\n", ids[0], data)

end:
	log.Printf("--------- Update() Test End --------\n")
	// Output:
}

func ExampleDB_BatchUpdate() {
	var err error
	var c redis.Conn
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"tel":"138*"*`
	dataMap := make(map[string]string)
	newDataMap := make(map[string]string)

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- BatchUpdate() Test Begin --------\n")
	log.Printf("Search pattern: %v, Result:\n", pattern)
	if ids, err = db.Search(c, pattern); err != nil {
		goto end
	}
	if dataMap, err = db.BatchGet(c, ids); err != nil {
		goto end
	}

	log.Printf("Result:\n")
	for id, data := range dataMap {
		log.Printf("id: %v, data: %v\n", id, data)
		newDataMap[id] = strings.Replace(data, "138", "186", -1)
	}

	if err = db.BatchUpdate(c, newDataMap); err != nil {
		goto end
	}

	if dataMap, err = db.BatchGet(c, ids); err != nil {
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
	var c redis.Conn
	var db *simpledb.DB
	ids := []string{}
	patterns := []string{
		// Search UTF8 string.
		`*"name":"王*宝"*`,
		// Search name matches Frank* and tel matches 13700137000.
		`*"name":"Frank*"*"tel":"13700137000"*`,
	}

	dataMap := make(map[string]string)

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Search() Test Begin --------\n")

	for _, p := range patterns {
		log.Printf("Search pattern: %v\n", p)
		if ids, err = db.Search(c, p); err != nil {
			goto end
		}

		if dataMap, err = db.BatchGet(c, ids); err != nil {
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
	var c redis.Conn
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

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- RegexpSearch() Test Begin --------\n")

	if ids, err = db.RegexpSearch(c, patterns); err != nil {
		goto end
	}

	for i, p := range patterns {
		log.Printf("Regexp pattern: %v\n", p)
		if dataMap, err = db.BatchGet(c, ids[i]); err != nil {
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
	var c redis.Conn
	var db *simpledb.DB
	infoMap := make(map[string]string)

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Info() Test Begin --------\n")

	if infoMap, err = db.Info(c); err != nil {
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
	var c redis.Conn
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"Frank Wang"*`
	data := ""

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- Delete() Test Begin --------\n")

	if ids, err = db.Search(c, pattern); err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	if data, err = db.Get(c, ids[0]); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", ids[0], data)

	// Delete
	if err = db.Delete(c, ids[0]); err != nil {
		goto end
	}

	// Search again after record deleted.
	if ids, err = db.Search(c, pattern); err != nil {
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
	var c redis.Conn
	var db *simpledb.DB
	ids := []string{}
	pattern := `*"name":"*"*`
	dataMap := make(map[string]string)

	if c, err = redis.Dial("tcp", ":6379"); err != nil {
		goto end
	}
	defer c.Close()

	db, _ = simpledb.Open(c, "student")

	log.Printf("\n")
	log.Printf("--------- BatchDelete() Test Begin --------\n")
	// Get all records.
	if ids, err = db.Search(c, pattern); err != nil {
		goto end
	}

	if dataMap, err = db.BatchGet(c, ids); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	for id, data := range dataMap {
		log.Printf("id: %v, data: %v\n", id, data)
	}

	// Delete all record.
	if err = db.BatchDelete(c, ids); err != nil {
		goto end
	}

	// Search again after record deleted.
	if ids, err = db.Search(c, pattern); err != nil {
		goto end
	}

	log.Printf("Search again after record deleted. Result:\n")
	log.Printf("ids: %v\n", ids)

end:
	log.Printf("--------- BatchDelete() Test End --------\n")
	// Output:
}
