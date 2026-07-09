//
// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package changestreams

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
)

type dialect int

const (
	dialectUnknown dialect = iota
	dialectGoogleSQL
	dialectPostgreSQL
)

func (d dialect) String() string {
	switch d {
	case dialectGoogleSQL:
		return "GoogleSQL"
	case dialectPostgreSQL:
		return "PostgreSQL"
	default:
		return ""
	}
}

func detectDialect(ctx context.Context, client *spanner.Client) (dialect, error) {
	var value string
	stmt := spanner.NewStatement("SELECT option_value FROM information_schema.database_options WHERE option_name = 'database_dialect'")
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		return r.ColumnByName("option_value", &value)
	}); err != nil {
		return dialectUnknown, err
	}

	switch strings.ToUpper(value) {
	case "GOOGLE_STANDARD_SQL", "":
		return dialectGoogleSQL, nil
	case "POSTGRESQL":
		return dialectPostgreSQL, nil
	default:
		return dialectUnknown, fmt.Errorf("invalid dialect: %q", value)
	}
}

type partitionMode int

const (
	partitionModeUnknown partitionMode = iota
	partitionModeImmutableKeyRange
	partitionModeMutableKeyRange
)

func (pm partitionMode) String() string {
	switch pm {
	case partitionModeImmutableKeyRange:
		return "IMMUTABLE_KEY_RANGE"
	case partitionModeMutableKeyRange:
		return "MUTABLE_KEY_RANGE"
	default:
		return "UNKNOWN"
	}
}

func detectPartitionMode(ctx context.Context, client *spanner.Client, streamID string) (partitionMode, error) {
	var value string
	found := false
	stmt := spanner.Statement{
		SQL: "SELECT option_value FROM information_schema.change_stream_options WHERE change_stream_name = @stream_id AND option_name = 'partition_mode'",
		Params: map[string]interface{}{
			"stream_id": streamID,
		},
	}
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		found = true
		return r.ColumnByName("option_value", &value)
	}); err != nil {
		return partitionModeUnknown, err
	}

	// If the partition_mode option is not found, default to IMMUTABLE_KEY_RANGE.
	if !found {
		return partitionModeImmutableKeyRange, nil
	}

	switch strings.ToUpper(value) {
	case "IMMUTABLE_KEY_RANGE", "":
		return partitionModeImmutableKeyRange, nil
	case "MUTABLE_KEY_RANGE":
		return partitionModeMutableKeyRange, nil
	default:
		return partitionModeUnknown, fmt.Errorf("invalid partition mode: %q", value)
	}
}
