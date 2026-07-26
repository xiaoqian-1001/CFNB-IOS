//go:build ios

package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
)

func main() {
	// Called by Go runtime on first exported function invocation.
	// Server runs in a goroutine from StartServer().
}

//export StartServer
func StartServer(port *C.char) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "Server panicked: %v\n", r)
				os.Stderr.Sync()
			}
		}()
		RunServer(C.GoString(port))
	}()
}
