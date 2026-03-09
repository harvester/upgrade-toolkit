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

package main

import (
	"flag"
	"fmt"

	"github.com/harvester/upgrade-toolkit/pkg/version"
)

// VersionCommand implements the version subcommand
type VersionCommand struct {
	fs *flag.FlagSet
}

func (c *VersionCommand) Name() string {
	return "version"
}

func (c *VersionCommand) FlagSet() *flag.FlagSet {
	if c.fs == nil {
		c.fs = flag.NewFlagSet("version", flag.ExitOnError)
	}
	return c.fs
}

func (c *VersionCommand) Run() error {
	fmt.Println(version.String())
	return nil
}
