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
	"github.com/googleapis/gax-go/v2"
	"regexp"
	"strings"
	"time"

	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	pbt "github.com/golang/protobuf/ptypes/timestamp"
	
)

// type DatabaseAdminClient struct {
// 	adminClient *vkit.DatabaseAdminClient
// }

// // NewDatabaseAdminClient creates a new database admin client.
// //
// // Cloud Spanner Database Admin API
// //
// // The Cloud Spanner Database Admin API can be used to create, drop, and
// // list databases. It also enables updating the schema of pre-existing
// // databases.
// func NewDatabaseAdminClient(ctx context.Context, opts ...option.ClientOption) (*DatabaseAdminClient, error) {
// 	client, err := vkit.NewDatabaseAdminClient(ctx, opts...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &DatabaseAdminClient{client}, nil
// }

// // Connection returns the client's connection to the API service.
// func (c *DatabaseAdminClient) Connection() *grpc.ClientConn {
// 	return c.adminClient.Connection()
// }

// // Close closes the connection to the API service. The user should invoke this when
// // the client is no longer required.
// func (c *DatabaseAdminClient) Close() error {
// 	return c.adminClient.Close()
// }

// // ListDatabases lists Cloud Spanner databases.
// func (c *DatabaseAdminClient) ListDatabases(ctx context.Context, req *databasepb.ListDatabasesRequest, opts ...gax.CallOption) *vkit.DatabaseIterator {
// 	return c.adminClient.ListDatabases(ctx, req, opts...)
// }

// // CreateDatabase creates a new Cloud Spanner database and starts to prepare it for serving.
// // The returned [long-running operation][google.longrunning.Operation] will
// // have a name of the format <database_name>/operations/<operation_id> and
// // can be used to track preparation of the database. The
// // [metadata][google.longrunning.Operation.metadata] field type is
// // [CreateDatabaseMetadata][google.spanner.admin.database.v1.CreateDatabaseMetadata]. The
// // [response][google.longrunning.Operation.response] field type is
// // [Database][google.spanner.admin.database.v1.Database], if successful.
// func (c *DatabaseAdminClient) CreateDatabase(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (*vkit.CreateDatabaseOperation, error) {
// 	return c.adminClient.CreateDatabase(ctx, req, opts...)
// }

// // GetDatabase gets the state of a Cloud Spanner database.
// func (c *DatabaseAdminClient) GetDatabase(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
// 	return c.adminClient.GetDatabase(ctx, req, opts...)

// }

// // UpdateDatabaseDdl updates the schema of a Cloud Spanner database by
// // creating/altering/dropping tables, columns, indexes, etc. The returned
// // [long-running operation][google.longrunning.Operation] will have a name of
// // the format <database_name>/operations/<operation_id> and can be used to
// // track execution of the schema change(s). The
// // [metadata][google.longrunning.Operation.metadata] field type is
// // [UpdateDatabaseDdlMetadata][google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata].  The operation has no response.
// func (c *DatabaseAdminClient) UpdateDatabaseDdl(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (*vkit.UpdateDatabaseDdlOperation, error) {
// 	return c.adminClient.UpdateDatabaseDdl(ctx, req, opts...)
// }

// // DropDatabase drops (aka deletes) a Cloud Spanner database.
// func (c *DatabaseAdminClient) DropDatabase(ctx context.Context, req *databasepb.DropDatabaseRequest, opts ...gax.CallOption) error {
// 	return c.adminClient.DropDatabase(ctx, req, opts...)
// }

// // GetDatabaseDdl returns the schema of a Cloud Spanner database as a list of formatted
// // DDL statements. This method does not show pending schema updates, those may
// // be queried using the [Operations][google.longrunning.Operations] API.
// func (c *DatabaseAdminClient) GetDatabaseDdl(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
// 	return c.adminClient.GetDatabaseDdl(ctx, req, opts...)
// }

// // SetIamPolicy sets the access control policy on a database resource.
// // Replaces any existing policy.
// //
// // Authorization requires spanner.databases.setIamPolicy
// // permission on [resource][google.iam.v1.SetIamPolicyRequest.resource].
// func (c *DatabaseAdminClient) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest, opts ...gax.CallOption) (*iampb.Policy, error) {
// 	return c.adminClient.SetIamPolicy(ctx, req, opts...)

// }

// // GetIamPolicy gets the access control policy for a database resource.
// // Returns an empty policy if a database exists but does
// // not have a policy set.
// //
// // Authorization requires spanner.databases.getIamPolicy permission on
// // [resource][google.iam.v1.GetIamPolicyRequest.resource].
// func (c *DatabaseAdminClient) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest, opts ...gax.CallOption) (*iampb.Policy, error) {
// 	return c.adminClient.GetIamPolicy(ctx, req, opts...)
// }

// // TestIamPermissions returns permissions that the caller has on the specified database resource.
// //
// // Attempting this RPC on a non-existent Cloud Spanner database will
// // result in a NOT_FOUND error if the user has
// // spanner.databases.list permission on the containing Cloud
// // Spanner instance. Otherwise returns an empty set of permissions.
// func (c *DatabaseAdminClient) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest, opts ...gax.CallOption) (*iampb.TestIamPermissionsResponse, error) {
// 	return c.adminClient.TestIamPermissions(ctx, req, opts...)
// }

func getInstanceNameFromDatabasePath(databasePath string) (instancePath string) {
	//Assumed that the input to this has been validated.
	// module uses validDatabaseName() to validate the database name.
	pathParts := strings.Split(databasePath, "/")
	projectsLabel := pathParts[0]
	projectName := pathParts[1]
	instanceLabel := pathParts[2]
	instanceName := pathParts[3]

	return fmt.Sprintf("%s/%s/%s/%s", projectsLabel, projectName, instanceLabel, instanceName)

}

// CreateNewBackup creates a new admin client to connect to a database,
// and creates a backup called backupID with expiry time of expireTime
// Required. The name of the instance in which the backup will be
// created. This must be the same instance that contains the database the
// backup will be created from. The backup will be stored in the
// location(s) specified in the instance configuration of this
// instance. Values are of the form
// `projects/<project>/instances/<instance>` .
// backup_id: str : The ID/name of the backup
// database_path: str : The database URI.
// expire_time: time: The timestamp after which the backup is eligible for
// deletion. The time can have microsecond granularity and must be at
// least 6 hours and at most 366 days from the time the CreateBackup
// request is processed.
func (c *DatabaseAdminClient) CreateNewBackup(ctx context.Context, backupID string, databasePath string, expireTime time.Time, opts ...gax.CallOption) (*CreateBackupOperation, error) {

	// Validate database path.
	if err := validDatabaseName(databasePath); err != nil {
		return nil, err
	}
	expireTimepb := timestampProto(expireTime)

	// create request from parameters
	req := &databasepb.CreateBackupRequest{
		Parent:   getInstanceNameFromDatabasePath(databasePath),
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
	validDBPattern = regexp.MustCompile("^projects/[^/]+/instances/[^/]+/databases/[^/]+$")
)

func validDatabaseName(db string) error {
	if matched := validDBPattern.MatchString(db); !matched {
		return fmt.Errorf("database name %q should conform to pattern %q",
			db, validDBPattern.String())
	}
	return nil
}