package simpledb

import (
	"log"
)

func debugPrintf(format string, values ...interface{}) {
	if DEBUG {
		log.Printf("[simpledb-debug] "+format, values...)
	}
}
