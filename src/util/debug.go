package util

import (
	"log"
)

const (
	debugMode = false
)

func Println(str string, a ...any) {
	if debugMode {
		str = str + "\n"
		log.Printf(str, a...)
	}
}
