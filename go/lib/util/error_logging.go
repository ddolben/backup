package util

import (
	"fmt"
)

const doPanic = true

func ErrorOrPanic(msg string, args ...any) error {
	errMsg := fmt.Sprintf(msg, args...)
	if doPanic {
		panic(errMsg)
	}
	return fmt.Errorf(errMsg)
}
