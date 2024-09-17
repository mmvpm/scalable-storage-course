package main

import (
	"fmt"
	"time"
)

func filler(b []byte, ifzero byte, ifnot byte) {
	for i := range b {
		if i == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}

func main() {
	b := make([]byte, 100)

	go func() {
		for {
			filler(b[:50], '0', '1')
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			filler(b[50:], 'X', 'Y')
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			fmt.Println(string(b))
			time.Sleep(1 * time.Second)
		}
	}()

	for {
	}
}
