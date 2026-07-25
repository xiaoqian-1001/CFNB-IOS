//go:build ios

package main

/*
#include <stdlib.h>
*/
import "C"

func main() {
	// Called by Go runtime on first exported function invocation.
	// Server runs in a goroutine from StartServer().
}

//export StartServer
func StartServer(port *C.char) {
	go RunServer(C.GoString(port))
}
