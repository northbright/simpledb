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
