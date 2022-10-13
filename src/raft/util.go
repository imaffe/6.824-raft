package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// Debugging
//const Debug = false
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug {
//		log.Printf(format, a...)
//	}
//	return
//}

var debugStart time.Time
var debugVerbosity int
var inited int32

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func Init() {
	if atomic.CompareAndSwapInt32(&inited, 0,1) {
		debugVerbosity = getVerbosity()
		debugStart = time.Now()
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
}


func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

