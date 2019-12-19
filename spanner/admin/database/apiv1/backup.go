/*
Copyright 2019 Google LLC

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
package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	pbt "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/googleapis/gax-go/v2"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// StartBackupOperation creates a backup of the given database. It will be stored
// as projects/<project>/instances/<instance>/backups/<backupID>. The
// backup will be automatically deleted by Cloud Spanner after its expiration.
//
// backupID must be unique across an instance.
//
// expires is the time the backup will expire. It is respected to
// microsecond granularity.
//
// The database must have the form
// projects/<project>/instances/<instance>/databases/<database>.
func (c *DatabaseAdminClient) StartBackupOperation(ctx context.Context, backupID string, database string, expires time.Time, opts ...gax.CallOption) (*CreateBackupOperation, error) {
	// Validate database path.
	validDBPattern := regexp.MustCompile("^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$")
	if matched := validDBPattern.MatchString(database); !matched {
		return nil, fmt.Errorf("database name %q should conform to pattern %q",
			database, validDBPattern.String())
	}
	expireTimepb := &pbt.Timestamp{Seconds: expires.Unix(), Nanos: int32(expires.Nanosecond())}
	databasePathFragments := strings.Split(database, "/")
	// Create request from parameters.
	req := &databasepb.CreateBackupRequest{
		Parent:   fmt.Sprintf("projects/%s/instances/%s", databasePathFragments[1], databasePathFragments[3]),
		BackupId: backupID,
		Backup: &databasepb.Backup{
			Database:   database,
			ExpireTime: expireTimepb,
		},
	}
	return c.CreateBackup(ctx, req, opts...)
}
