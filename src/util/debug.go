package util

import (
	"fmt"
	"log"
)

const (
	debugMode = true
)

func Println(str string, a ...any) {
	if debugMode {
		str = str + "\n"
		log.Printf(str, a...)
	}
}

func Debug(sevr int, str string, a ...any) {
	if debugMode {
		str = str + "\n"
		prefix := fmt.Sprintf("SEVER(%d): ", sevr)
		log.Printf(prefix+str, a...)
	}
}
