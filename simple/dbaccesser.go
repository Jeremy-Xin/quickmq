package main

import (
	"fmt"
	"strconv"
)

const (
	keyMaxSizeSuffix    = "max_size"
	keyPutPointerSuffix = "put_pointer"
	keyGetPointerSuffix = "get_pointer"
)

func keyMaxSize(queueName string) string {
	return fmt.Sprintf("%s.%s", queueName, keyMaxSizeSuffix)
}

func keyPutPtr(queueName string) string {
	return fmt.Sprintf("%s.%s", queueName, keyPutPointerSuffix)
}

func keyGetPtr(queueName string) string {
	return fmt.Sprintf("%s.%s", queueName, keyGetPointerSuffix)
}

func readQueueMeta(queueName string) []string {
	keyMaxqueue := keyMaxSize(queueName)
	maxqueue, _ := db.Get([]byte(keyMaxqueue), nil)
	if len(maxqueue) == 0 {
		maxqueue = []byte(strconv.Itoa(*defaultMaxqueue))
	}
	keyPutPointer := keyPutPtr(queueName)
	putPointer, _ := db.Get([]byte(keyPutPointer), nil)
	keyGetPointer := keyGetPtr(queueName)
	getPointer, _ := db.Get([]byte(keyGetPointer), nil)

	return []string{string(maxqueue), string(putPointer), string(getPointer)}
}

func readQueueGetPtr(queueName string) string {
	metadata := readQueueMeta(queueName)

	maxsize, _ := strconv.Atoi(metadata[0])
	putptr, _ := strconv.Atoi(metadata[1])
	getptr, _ := strconv.Atoi(metadata[2])

	if getptr == putptr {
		// all data in queue has been get
		return "0"
	}
	getptr++
	if getptr > maxsize {
		getptr = 1
	}

	newGetptr := strconv.Itoa(getptr)
	db.Put([]byte(keyGetPtr(queueName)), []byte(newGetptr), nil)
	return newGetptr
}

func readQueuePutPtr(queueName string) string {
	metadata := readQueueMeta(queueName)
	maxsize, _ := strconv.Atoi(metadata[0])
	putptr, _ := strconv.Atoi(metadata[1])
	getptr, _ := strconv.Atoi(metadata[2])

	var newPutptr string
	putptr++
	if putptr == getptr {
		return "0"
	}
	if getptr <= 1 && putptr > maxsize {
		return "0"
	}
	if putptr > maxsize {
		newPutptr = "1"
	} else {
		newPutptr = strconv.Itoa(putptr)
	}

	db.Put([]byte(keyPutPtr(queueName)), []byte(newPutptr), nil)
	return newPutptr
}
