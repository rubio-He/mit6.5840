package util

import (
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
