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

	data := []string{
		`{"name":"Frank Xu","tel":"13800138000"}`,
		`{"name":"Frank Xu","tel":"13800138000"}`, // repeated
		`{"name":"Frank Wang","tel":"13600136000"}`,
		`{"name":"张三","tel":"13800138001"}`,
		`{"name":"李四","tel":"13800138002"}`,
		`{"name":"王大宝","tel":"13800138003"}`,
		`{"name":"王小宝","tel":"13800138003"}`,
	}

	for i := 0; i < len(data); i++ {
		id, err := db.Create(c, data[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "db.Create() error: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "db.Create() ok: id: %v\n", id)
		}
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

	// Search name matches Frank*
	ids, err = db.Search(c, `*"name":"Frank*"*`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Search() error: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "db.Search) ok: ids: %v\n", ids)
	}

	// Search name matches UTF8 string.
	ids, err = db.Search(c, `*"name":"王*宝"*`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Search() error: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "db.Search) ok: ids: %v\n", ids)
	}

	// Search tel matches 13800138003
	ids, err = db.Search(c, `*"tel":"13800138003"*`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Search() error: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "db.Search) ok: ids: %v\n", ids)
	}

	// Search name matches Frank* and tel matches 13800138000.
	ids, err = db.Search(c, `*"name":"Frank*"*"tel":"13800138000"*`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db.Search() error: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "db.Search) ok: ids: %v\n", ids)
	}

	// Output:
}
