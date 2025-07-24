package logger

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// NewLogger instantiate zerolog configuration
func NewLogger() *zerolog.Logger {
	var logger zerolog.Logger
	switch strings.TrimSpace(os.Getenv("RAFTY_LOG_LEVEL")) {
	case "panic":
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	if strings.TrimSpace(os.Getenv("RAFTY_LOG_FORMAT_JSON")) == "" {
		output := zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true, TimeFormat: time.RFC3339}
		output.FormatLevel = func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("| %s |", i))
		}
		output.FormatMessage = func(i interface{}) string {
			return fmt.Sprintf("%s", i)
		}

		logger = zerolog.New(output).With().Timestamp().Caller().Logger()
	} else {
		logger = zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	}
	return &logger
}
