package simpledb_test

import (
	"log"
	"strings"

	//"github.com/garyburd/redigo/redis"
	"github.com/northbright/simpledb"
)

func ExampleDB_Create() {
	var err error
	var db *simpledb.DB
	var record = simpledb.Record{}
	id := ""
	data := `{"name":"Bob Smith","tel":"13500135000"}`

	log.Printf("\n")
	log.Printf("--------- Create() Test Begin --------\n")

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	if id, err = db.Create(data); err != nil {
		goto end
	}

	if record, err = db.Get(id); err != nil {
		goto end
	}

	log.Printf("Result: id: %v, data: %v\n", record.ID, record.Data)
end:
	log.Printf("--------- Create() Test End --------\n")
	// Output:
}

func ExampleDB_BatchCreate() {
	var err error
	var db *simpledb.DB
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
	records := []simpledb.Record{}

	log.Printf("\n")
	log.Printf("--------- BatchCreate() Test Begin --------\n")

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	if ids, err = db.BatchCreate(data); err != nil {
		goto end
	}

	log.Printf("Result:\n")
	if records, err = db.BatchGet(ids); err != nil {
		goto end
	}

	for _, r := range records {
		log.Printf("id: %v, data: %v\n", r.ID, r.Data)
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

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	if exists, err = db.IDExists(id); err != nil {
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
	record := simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- Get() Test Begin --------\n")
	if ids, err = db.Search(pattern); err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	if record, err = db.Get(ids[0]); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", record.ID, record.Data)

end:
	log.Printf("--------- Get() Test End --------\n")
	// Output:
}

func ExampleDB_BatchGet() {
	var err error
	var db *simpledb.DB

	ids := []string{}
	pattern := `*"name":"王*"*`
	records := []simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- BatchGet() Test Begin --------\n")
	if ids, err = db.Search(pattern); err != nil {
		goto end
	}
	if records, err = db.BatchGet(ids); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	for _, record := range records {
		log.Printf("id: %v, data: %v\n", record.ID, record.Data)
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
	record := simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- Update() Test Begin --------\n")
	if ids, err = db.Search(pattern); err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	if record, err = db.Get(ids[0]); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", record.ID, record.Data)

	// Change phone number from 135xxx to 138xxx.
	record.Data = strings.Replace(record.Data, "135", "158", -1)
	if err = db.Update(record); err != nil {
		goto end
	}

	log.Printf("Change phone number from 135xxx to 158xxx.")
	if record, err = db.Get(ids[0]); err != nil {
		goto end
	}
	log.Printf("Updated. id: %v, data: %v\n", record.ID, record.Data)

end:
	log.Printf("--------- Update() Test End --------\n")
	// Output:
}

func ExampleDB_BatchUpdate() {
	var err error
	var db *simpledb.DB

	ids := []string{}
	pattern := `*"tel":"138*"*`
	oldRecords := []simpledb.Record{}
	newRecords := []simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- BatchUpdate() Test Begin --------\n")
	log.Printf("Search pattern: %v, Result:\n", pattern)
	if ids, err = db.Search(pattern); err != nil {
		goto end
	}
	if oldRecords, err = db.BatchGet(ids); err != nil {
		goto end
	}

	log.Printf("Result:\n")
	for _, r := range oldRecords {
		log.Printf("id: %v, data: %v\n", r.ID, r.Data)
		newRecords = append(newRecords, simpledb.Record{ID: r.ID, Data: strings.Replace(r.Data, "138", "186", -1)})
	}

	if err = db.BatchUpdate(newRecords); err != nil {
		goto end
	}

	if newRecords, err = db.BatchGet(ids); err != nil {
		goto end
	}

	log.Printf("Batch update phone number from 138xxx to 186xxx.")
	for _, r := range newRecords {
		log.Printf("id: %v, data: %v\n", r.ID, r.Data)
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
		// Empty pattern: return all record ID.
		``,
	}

	records := []simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- Search() Test Begin --------\n")

	for _, p := range patterns {
		log.Printf("Search pattern: %v\n", p)
		if ids, err = db.Search(p); err != nil {
			goto end
		}

		if records, err = db.BatchGet(ids); err != nil {
			goto end
		}

		log.Printf("Result:\n")
		for _, r := range records {
			log.Printf("id: %v, data: %v\n", r.ID, r.Data)
		}

		log.Printf("\n")
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

	records := []simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- RegexpSearch() Test Begin --------\n")

	if ids, err = db.RegexpSearch(patterns); err != nil {
		goto end
	}

	for i, p := range patterns {
		log.Printf("Regexp pattern: %v\n", p)
		if records, err = db.BatchGet(ids[i]); err != nil {
			goto end
		}

		log.Printf("Result:\n")
		for _, r := range records {
			log.Printf("id: %v, data: %v\n", r.ID, r.Data)
		}
	}
end:
	log.Printf("--------- RegexpSearch() Test End --------\n")
	// Output:
}

func ExampleDB_Count() {
	var err error
	var db *simpledb.DB

	var count uint64

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- Count() Test Begin --------\n")

	if count, err = db.Count(); err != nil {
		goto end
	}
	log.Printf("Record count: %v\n", count)
end:
	log.Printf("--------- Count() Test End --------\n")
	// Output:

}

func ExampleDB_Info() {
	var err error
	var db *simpledb.DB

	infoMap := make(map[string]string)

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- Info() Test Begin --------\n")

	if infoMap, err = db.Info(); err != nil {
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
	record := simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- Delete() Test Begin --------\n")

	if ids, err = db.Search(pattern); err != nil {
		goto end
	}

	if len(ids) != 1 {
		goto end
	}

	if record, err = db.Get(ids[0]); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	log.Printf("id: %v, data: %v\n", record.ID, record.Data)

	// Delete
	if err = db.Delete(ids[0]); err != nil {
		goto end
	}

	// Search again after record deleted.
	if ids, err = db.Search(pattern); err != nil {
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
	records := []simpledb.Record{}

	db, _ = simpledb.Open(":6379", "", "student")
	defer db.Close()

	log.Printf("\n")
	log.Printf("--------- BatchDelete() Test Begin --------\n")
	// Get all records.
	if ids, err = db.Search(pattern); err != nil {
		goto end
	}

	if records, err = db.BatchGet(ids); err != nil {
		goto end
	}

	log.Printf("Search pattern: %v, Result:\n", pattern)
	for _, r := range records {
		log.Printf("id: %v, data: %v\n", r.ID, r.Data)
	}

	// Delete all record.
	if err = db.BatchDelete(ids); err != nil {
		goto end
	}

	// Search again after record deleted.
	if ids, err = db.Search(pattern); err != nil {
		goto end
	}

	log.Printf("Search again after record deleted. Result:\n")
	log.Printf("ids: %v\n", ids)

end:
	log.Printf("--------- BatchDelete() Test End --------\n")
	// Output:
}
