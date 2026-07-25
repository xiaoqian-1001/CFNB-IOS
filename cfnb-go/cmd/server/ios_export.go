//go:build carchive

package main

/*
#include <stdlib.h>
*/
import "C"

//export StartServer
func StartServer(port *C.char) {
	go RunServer(C.GoString(port))
}
