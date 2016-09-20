package simpledb_test

import (
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"
	"github.com/northbright/simpledb"
)

func ExampleDB_Create() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")

	data := `{"name":"Bob Smith","tel":"13500135000"}`

	id, err := db.Create(c, data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Create() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.Create() ok: id: %v\n", id)

	// Output:
}

func ExampleDB_BatchCreate() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")

	data := []string{
		`{"name":"Frank Xu","tel":"13800138000"}`,
		`{"name":"Frank Wang","tel":"13600136000"}`,
		`{"name":"张三","tel":"13800138001"}`,
		`{"name":"李四","tel":"13800138002"}`,
		`{"name":"王大宝","tel":"13800138003"}`,
		`{"name":"王小宝","tel":"13800138003"}`,
	}

	ids, err := db.BatchCreate(c, data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.BatchCreate() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.BatchCreate() ok: id: %v\n", ids)

	// Output:
}

func ExampleDB_IdExists() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")

	id := "1"
	exists, recordHashKey, indexHashKey, recordHashField, err := db.IdExists(c, id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "IdExists() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "IdExists() ok: exists: %v, recordHashKey: %v, indexHashKey: %v, recordHashField: %v\n", exists, recordHashKey, indexHashKey, recordHashField)

	// Output:
}

func ExampleDB_Get() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")

	id := "1"
	data, err := db.Get(c, id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Get() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.Get() ok. id: %v, data: %v\n", id, data)

	// Output:
}

func ExampleDB_BatchGet() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")
	ids := []string{}

	// Search name matches Frank*
	ids, err = db.Search(c, `*"name":"Frank*"*`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Search() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.Search() ok: ids: %v\n", ids)

	dataMap, err := db.BatchGet(c, ids)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.BatchGet() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.BatchGet() ok.\n")
	for id, data := range dataMap {
		fmt.Fprintf(os.Stderr, "id: %v, data: %v\n", id, data)
	}

	// Output:
}

func ExampleDB_Update() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")

	// Update Frank's telephone number.
	newData := `{"name":"Frank Xu","tel":"13600136006"}`
	err = db.Update(c, "2", newData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Upate() error: %v\n", err)
		return
	}

	// Output:
}

func ExampleDB_BatchUpdate() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")

	m := make(map[string]string)
	m["2"] = `{"name":"Frank Xu","tel":"13800138000"}`
	m["3"] = `{"name":"Frank Wang","tel":"13700137000"}`

	err = db.BatchUpdate(c, m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.BatchUpate() error: %v\n", err)
		return
	}

	// Output:
}

func ExampleDB_Search() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis.Dial() error: %v\n", err)
		return
	}
	defer c.Close()

	db := simpledb.Open("student")
	ids := []string{}

	// Search name matches UTF8 string.
	ids, err = db.Search(c, `*"name":"王*宝"*`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Search() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.Search) ok: ids: %v\n", ids)

	// Test BatchGet()
	dataMap, err := db.BatchGet(c, ids)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.BatchGet() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.BatchGet() ok.\n")
	for k, v := range dataMap {
		fmt.Fprintf(os.Stderr, "id: %v, data: %v\n", k, v)
	}

	// Search name matches Frank* and tel matches 13800138000.
	ids, err = db.Search(c, `*"name":"Frank*"*"tel":"13800138000"*`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Search() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.Search) ok: ids: %v\n", ids)

	// Test BatchGet()
	dataMap, err = db.BatchGet(c, ids)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.BatchGet() error: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "db.BatchGet() ok.\n")
	for k, v := range dataMap {
		fmt.Fprintf(os.Stderr, "id: %v, data: %v\n", k, v)
	}

	// Output:
}
