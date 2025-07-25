package logger

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestSetLoggerLogLevel(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		logLevel string
		expected string
	}{
		{
			logLevel: "info",
			expected: "info",
		},
		{
			logLevel: "warn",
			expected: "warn",
		},
		{
			logLevel: "debug",
			expected: "debug",
		},
		{
			logLevel: "error",
			expected: "error",
		},
		{
			logLevel: "fatal",
			expected: "fatal",
		},
		{
			logLevel: "trace",
			expected: "trace",
		},
		{
			logLevel: "panic",
			expected: "panic",
		},
		{
			logLevel: "plop",
			expected: "info",
		},
	}

	for _, tc := range tests {
		_ = os.Setenv("RAFTY_LOG_LEVEL", tc.logLevel)
		log.Logger = *NewLogger()

		assert.Equal(tc.expected, zerolog.GlobalLevel().String())
		_ = os.Unsetenv("RAFTY_LOG_LEVEL")

		_ = os.Setenv("RAFTY_LOG_LEVEL", tc.logLevel)
		_ = os.Setenv("RAFTY_LOG_FORMAT_JSON", "true")
		defer func() {
			_ = os.Unsetenv("RAFTY_LOG_FORMAT_JSON")
		}()
		log.Logger = *NewLogger()

		assert.Equal(tc.expected, zerolog.GlobalLevel().String())
		_ = os.Unsetenv("RAFTY_LOG_LEVEL")
	}
}

func TestLogger_info(t *testing.T) {
	log.Logger = *NewLogger()
	log.Info().Msgf("Testing logger")
}
