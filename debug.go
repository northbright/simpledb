package simpledb

import (
	"log"
)

func DebugPrintf(format string, values ...interface{}) {
	if DEBUG {
		log.Printf("[simpledb-debug] "+format, values...)
	}
}
