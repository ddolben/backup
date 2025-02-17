package logging

import "log"

type LogLevel int

const (
	Error LogLevel = iota
	Info
	Verbose
	Debug
)

type Logger interface {
	Errorf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Verbosef(msg string, args ...interface{})
	Debugf(msg string, args ...interface{})
}

type DefaultLogger struct {
	Level LogLevel
}

func (l *DefaultLogger) Errorf(msg string, args ...interface{}) {
	if l.Level >= Error {
		log.Printf("[ERROR] "+msg, args...)
	}
}

func (l *DefaultLogger) Infof(msg string, args ...interface{}) {
	if l.Level >= Info {
		log.Printf("[INFO] "+msg, args...)
	}
}

func (l *DefaultLogger) Verbosef(msg string, args ...interface{}) {
	if l.Level >= Verbose {
		log.Printf("[VERBOSE] "+msg, args...)
	}
}

func (l *DefaultLogger) Debugf(msg string, args ...interface{}) {
	if l.Level >= Debug {
		log.Printf("[DEBUG] "+msg, args...)
	}
}
