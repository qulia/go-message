// Package log provides functionality to centralize logging, support different levels of logging
package log

import (
	slog "log"
	"path"
	"runtime"
)

// Level type is used for logging level
type Level int

const (
	// None disables logging
	None Level = -1 + iota
	// Error and up will get logged
	Error
	// Warning and up will get logged
	Warning
	// Verbose enables all logging
	Verbose
)

var (
	// CurrentLevel sets the effective logging
	// Use the package Level type to set values
	currentLevel = Verbose
)

// SetLevel sets the logging level
func SetLevel(l Level) {
	currentLevel = l
}

// V Verbose logging
func V(format string, args ...interface{}) {
	if currentLevel >= Verbose {
		slog.Printf("%s#%d: "+format, prependCaller(args)...)
	}
}

// W Warning logging
func W(format string, args ...interface{}) {
	if currentLevel >= Warning {
		slog.Printf("%s#%d: "+format, prependCaller(args)...)
	}
}

// E Error logging
func E(err error, format string, args ...interface{}) {
	if err == nil {
		return
	}
	if currentLevel >= Error {
		argsWithError := append(args, err)
		slog.Printf("%s#%d: "+format+" %s", prependCaller(argsWithError)...)
	}
}

// F Fatal logging
func F(err error, format string, args ...interface{}) {
	if err == nil {
		return
	}
	slog.Fatalf(format+" %s", append(args, err)...)
}

func prependCaller(args []interface{}) []interface{} {
	_, file, line, _ := runtime.Caller(2)
	logFlags := slog.Flags()
	if logFlags&slog.Lshortfile != 0 {
		file = path.Base(file)
	}
	res := append([]interface{}{file, line}, args...)
	return res
}
