/*
Copyright 2017 Google LLC

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
	"time"

	"github.com/googleapis/gax-go/v2"

	pbt "github.com/golang/protobuf/ptypes/timestamp"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// CreateNewBackup creates a new admin client to connect to a database,
// and creates a backup called backupID with expiry time of expireTime
// Required. The name of the instance in which the backup will be
// created. This must be the same instance that contains the database the
// backup will be created from. The backup will be stored in the
// location(s) specified in the instance configuration of this
// instance.
func (c *DatabaseAdminClient) CreateNewBackup(ctx context.Context, backupID string, databasePath string, expireTime time.Time, opts ...gax.CallOption) (*CreateBackupOperation, error) {
	// Validate database path.
	project, instance, _, err := validDatabaseName(databasePath)
	if err != nil {
		return nil, err
	}
	expireTimepb := timestampProto(expireTime)
	// Create request from parameters.
	req := &databasepb.CreateBackupRequest{
		Parent:   DatabaseAdminInstancePath(project, instance),
		BackupId: backupID,
		Backup: &databasepb.Backup{
			Database:   databasePath,
			ExpireTime: expireTimepb,
		},
	}
	return c.CreateBackup(ctx, req)
}

// timestampProto takes a time.Time and converts it into pbt.Timestamp for
// calling gRPC APIs.
func timestampProto(t time.Time) *pbt.Timestamp {
	return &pbt.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}

var (
	validDBPattern = regexp.MustCompile("^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$")
)

// validDatabaseName uses validDBPattern to validate that the database name
// conforms to the required pattern and extracts the relevant names.
func validDatabaseName(db string) (project string, instance string, database string, err error) {
	if matched := validDBPattern.MatchString(db); !matched {
		return "", "", "", fmt.Errorf("database name %q should conform to pattern %q",
			db, validDBPattern.String())
	}
	return validDBPattern.ReplaceAllString(db, "${project}"), validDBPattern.ReplaceAllString(db, "${instance}"), validDBPattern.ReplaceAllString(db, "${database}"), nil
}
