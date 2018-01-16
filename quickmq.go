package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"runtime"
	"strconv"

	"github.com/valyala/fasthttp"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var db *leveldb.DB
var defaultMaxqueue, keepalive, cpu, cacheSize, writeBuffer *int
var ip, port, dbPath *string
var verbose *bool
var sync = &opt.WriteOptions{Sync: true}
var putnamechan = make(chan string, 100)
var putptrchan = make(chan string, 100)
var getnamechan = make(chan string, 100)
var getptrchan = make(chan string, 100)

func init() {
	defaultMaxqueue = flag.Int("maxqueue", 1000000, "the max queue length")
	ip = flag.String("ip", "0.0.0.0", "ip address to listen on")
	port = flag.String("port", "1218", "port to listen on")
	dbPath = flag.String("db", "level.db", "database path")
	cacheSize = flag.Int("cache", 64, "cache size(MB)")
	writeBuffer = flag.Int("buffer", 32, "write buffer(MB)")
	cpu = flag.Int("cpu", runtime.NumCPU(), "cpu number for httpmq")
	keepalive = flag.Int("k", 60, "keepalive timeout for httpmq")
	flag.Parse()

	var err error
	db, err = leveldb.OpenFile(*dbPath, &opt.Options{BlockCacheCapacity: *cacheSize,
		WriteBuffer: *writeBuffer * 1024 * 1024})
	if err != nil {
		log.Fatalln("db.Get(), err:", err)
	}
}

func handler(ctx *fasthttp.RequestCtx) {
	var data string
	var buf []byte
	name := string(ctx.FormValue("name"))
	opt := string(ctx.FormValue("opt"))
	pos := string(ctx.FormValue("pos"))
	num := string(ctx.FormValue("num"))
	charset := string(ctx.FormValue("charset"))

	method := string(ctx.Method())
	if method == "GET" {
		data = string(ctx.FormValue("data"))
	} else if method == "POST" {
		if string(ctx.Request.Header.ContentType()) == "application/x-www-form-urlencoded" {
			data = string(ctx.FormValue("data"))
		} else {
			buf = ctx.PostBody()
		}
	}

	if len(name) == 0 || len(opt) == 0 {
		ctx.Write([]byte("HTTPMQ_ERROR"))
		return
	}

	ctx.Response.Header.Set("Connection", "keep-alive")
	ctx.Response.Header.Set("Cache-Control", "no-cache")
	ctx.Response.Header.Set("Content-type", "text/plain")

	if len(charset) > 0 {
		ctx.Response.Header.Set("Content-type", "text/plain; charset="+charset)
	}

	if opt == "put" {
		if len(data) == 0 && len(buf) == 0 {
			ctx.Write([]byte("HTTPMQ_PUT_ERROR"))
			return
		}

		putnamechan <- name
		putpos := <-putptrchan

		if putpos != "0" {
			queueName := name + putpos
			if data != "" {
				db.Put([]byte(queueName), []byte(data), nil)
			} else if len(buf) > 0 {
				db.Put([]byte(queueName), buf, nil)
			}
			ctx.Response.Header.Set("Pos", putpos)
			ctx.Write([]byte("HTTPMQ_PUT_OK"))
		} else {
			ctx.Write([]byte("HTTPMQ_PUT_END"))
		}
	} else if opt == "get" {
		getnamechan <- name
		getpos := <-getptrchan

		if getpos == "0" {
			ctx.Write([]byte("HTTPMQ_GET_END"))
		} else {
			queueName := name + getpos
			v, err := db.Get([]byte(queueName), nil)
			if err == nil {
				ctx.Response.Header.Set("Pos", getpos)
				ctx.Write(v)
			} else {
				ctx.Write([]byte("HTTPMQ_GET_ERROR"))
			}
		}
	} else if opt == "status" {
		metadata := readQueueMeta(name)
		maxqueue, _ := strconv.Atoi(metadata[0])
		putpos, _ := strconv.Atoi(metadata[1])
		getpos, _ := strconv.Atoi(metadata[2])

		var ungetnum float64
		var putTimes, getTimes string
		if putpos >= getpos {
			ungetnum = math.Abs(float64(putpos - getpos))
			putTimes = "1st lap"
			getTimes = "1st lap"
		} else if putpos < getpos {
			ungetnum = math.Abs(float64(maxqueue - getpos + putpos))
			putTimes = "2nd lap"
			getTimes = "1st lap"
		}

		buf := fmt.Sprintf("------------------------------\n")
		buf += fmt.Sprintf("Queue Name: %s\n", name)
		buf += fmt.Sprintf("Maximum number of queues: %d\n", maxqueue)
		buf += fmt.Sprintf("Put position of queue (%s): %d\n", putTimes, putpos)
		buf += fmt.Sprintf("Get position of queue (%s): %d\n", getTimes, getpos)
		buf += fmt.Sprintf("Number of unread queue: %g\n\n", ungetnum)

		ctx.Write([]byte(buf))
	} else if opt == "view" {
		v, err := db.Get([]byte(name+pos), nil)
		if err == nil {
			ctx.Write([]byte(v))
		} else {
			ctx.Write([]byte("HTTPMQ_VIEW_ERROR"))
		}
	} else if opt == "reset" {
		maxqueue := strconv.Itoa(*defaultMaxqueue)
		db.Put([]byte(name+".maxqueue"), []byte(maxqueue), sync)
		db.Put([]byte(name+".putpos"), []byte("0"), sync)
		db.Put([]byte(name+".getpos"), []byte("0"), sync)
		ctx.Write([]byte("HTTPMQ_RESET_OK"))
	} else if opt == "maxqueue" {
		maxqueue, _ := strconv.Atoi(num)
		if maxqueue > 0 && maxqueue <= 10000000 {
			db.Put([]byte(name+".maxqueue"), []byte(num), sync)
			ctx.Write([]byte("HTTPMQ_MAXQUEUE_OK"))
		} else {
			ctx.Write([]byte("HTTPMQ_MAXQUEUE_CANCLE"))
		}
	}
}

func main() {
	runtime.GOMAXPROCS(*cpu)

	go func(namechan chan string, ptrchan chan string) {
		for {
			queueName := <-namechan
			putpos := readQueuePutPtr(queueName)
			ptrchan <- putpos
		}
	}(putnamechan, putptrchan)

	go func(namechan chan string, ptrchan chan string) {
		for {
			quequeName := <-namechan
			getpos := readQueueGetPtr(quequeName)
			ptrchan <- getpos
		}
	}(getnamechan, getptrchan)

	fmt.Printf("Quickmq is running on %s", *ip+":"+*port)
	fasthttp.ListenAndServe(*ip+":"+*port, handler)

}
