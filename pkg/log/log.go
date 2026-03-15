/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package log

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// FormatConsole is the log format value for human-readable console output.
const FormatConsole = "console"

// NewLogger creates a logr.Logger backed by zap.
// When development is true, output is human-readable console format.
// When development is false (production), output is JSON.
// The level parameter controls the logr verbosity level (0 = info, higher = more verbose).
func NewLogger(development bool, level int) (logr.Logger, error) {
	var cfg zap.Config
	if development {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}

	// Map logr verbosity to zap level: logr V(0) = zap InfoLevel (0),
	// V(1) = zap DebugLevel (-1), V(2) = zap level -2, etc.
	cfg.Level = zap.NewAtomicLevelAt(zapcore.Level(-level))

	zapLog, err := cfg.Build()
	if err != nil {
		return logr.Discard(), err
	}

	return zapr.NewLogger(zapLog), nil
}
