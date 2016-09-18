package simpledb

import (
//	"fmt"
//	"strconv"
)

func ComputeBucketId(id uint64) uint64 {
	if HashMaxZiplistEntries == 0 {
		return 1
	}
	return id/HashMaxZiplistEntries + 1
}
