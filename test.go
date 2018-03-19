package main

import (
	"encoding/json"
	"fmt"
)

type Proto struct {
	Header int32
	// Body   []byte
}

func main() {
	fmt.Println("hello!")
	p := new(Proto)
	p.Header = 1
	// p.Body = make([]byte, 8, 8)
	header, err := json.Marshal(p)
	if err != nil {

	}

	fmt.Println(len(header))
	fmt.Println(header)
}
