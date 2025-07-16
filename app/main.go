package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
)

var count atomic.Int64

func main() {
	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		log.Println("Accepted connection")
		go func(conn net.Conn) {
			defer conn.Close()
			processor := NewProcessor(conn)
			for {
				err := processor.Process()
				if err != nil {
					if err == io.EOF {
						return
					}
					log.Printf("an error has occurred %v\n", err)
					return
				}

				continue
			}
		}(conn)
	}
}
