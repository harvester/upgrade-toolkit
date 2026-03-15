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
	"testing"
)

func TestNewLoggerProduction(t *testing.T) {
	logger, err := NewLogger(false, 0)
	if err != nil {
		t.Fatalf("unexpected error creating production logger: %v", err)
	}
	if !logger.Enabled() {
		t.Error("production logger at level 0 should be enabled")
	}
}

func TestNewLoggerDevelopment(t *testing.T) {
	logger, err := NewLogger(true, 0)
	if err != nil {
		t.Fatalf("unexpected error creating development logger: %v", err)
	}
	if !logger.Enabled() {
		t.Error("development logger at level 0 should be enabled")
	}
}

func TestNewLoggerVerbosity(t *testing.T) {
	logger, err := NewLogger(false, 2)
	if err != nil {
		t.Fatalf("unexpected error creating logger with verbosity 2: %v", err)
	}

	// V(2) should be enabled since we set level=2
	if !logger.V(2).Enabled() {
		t.Error("logger with level=2 should have V(2) enabled")
	}

	// V(3) should not be enabled
	if logger.V(3).Enabled() {
		t.Error("logger with level=2 should not have V(3) enabled")
	}
}
