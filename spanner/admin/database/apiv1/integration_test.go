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

/*
This file holds tests for the in-memory fake for comparing it against a real Cloud Spanner.

By default it uses the Spanner client against the in-memory fake; set the
-test_db flag to "projects/P/instances/I/databases/D" to make it use a real
Cloud Spanner database instead. You may need to provide -timeout=5m too.
*/
package database

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"cloud.google.com/go/internal/uid"
	"cloud.google.com/go/spanner"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc"
)

var (
	// testProjectID specifies the project used for testing. It can be changed
	// by setting environment variable GCLOUD_TESTS_GOLANG_PROJECT_ID.
	testProjectID    = testutil.ProjID()
	testInstanceName = os.Getenv("GCLOUD_TESTS_GOLANG_INSTANCE_NAME")
	testDatabaseName = os.Getenv("GCLOUD_TESTS_GOLANG_DATABASE_NAME")
	testEndpoint     = os.Getenv("GCLOUD_TESTS_GOLANG_ENDPOINT")

	dbNameSpace       = uid.NewSpace("gotest", &uid.Options{Sep: '_', Short: true})
	instanceNameSpace = uid.NewSpace("gotest", &uid.Options{Sep: '-', Short: true})

	testTable        = "TestTable"
	testTableIndex   = "TestTableByValue"
	testTableColumns = []string{"Key", "StringValue"}

	databaseAdmin *DatabaseAdminClient
	instanceAdmin *instance.InstanceAdminClient

	singerDBStatements = []string{
		`CREATE TABLE Singers (
				SingerId	INT64 NOT NULL,
				FirstName	STRING(1024),
				LastName	STRING(1024),
				SingerInfo	BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
		`CREATE INDEX SingerByName ON Singers(FirstName, LastName)`,
		`CREATE TABLE Accounts (
				AccountId	INT64 NOT NULL,
				Nickname	STRING(100),
				Balance		INT64 NOT NULL,
			) PRIMARY KEY (AccountId)`,
		`CREATE INDEX AccountByNickname ON Accounts(Nickname) STORING (Balance)`,
		`CREATE TABLE Types (
				RowID		INT64 NOT NULL,
				String		STRING(MAX),
				StringArray	ARRAY<STRING(MAX)>,
				Bytes		BYTES(MAX),
				BytesArray	ARRAY<BYTES(MAX)>,
				Int64a		INT64,
				Int64Array	ARRAY<INT64>,
				Bool		BOOL,
				BoolArray	ARRAY<BOOL>,
				Float64		FLOAT64,
				Float64Array	ARRAY<FLOAT64>,
				Date		DATE,
				DateArray	ARRAY<DATE>,
				Timestamp	TIMESTAMP,
				TimestampArray	ARRAY<TIMESTAMP>,
			) PRIMARY KEY (RowID)`,
	}
)

const (
	str1 = "alice"
	str2 = "a@example.com"
)

var grpcHeaderChecker = testutil.DefaultHeadersEnforcer()

func initIntegrationTests() (cleanup func()) {
	ctx := context.Background()
	flag.Parse() // Needed for testing.Short().

	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if testProjectID == "" {
		log.Println("Integration tests skipped: GCLOUD_TESTS_GOLANG_PROJECT_ID is missing")
		return noop
	}

	ts := testutil.TokenSource(ctx, spanner.AdminScope, spanner.Scope)
	if ts == nil {
		log.Printf("Integration test skipped: cannot get service account credential from environment variable %v", "GCLOUD_TESTS_GOLANG_KEY")
		return noop
	}
	var err error
	//check if a specific endpoint is set for the integration test
	if testEndpoint == "" {
		testEndpoint = "spanner.googleapis.com:443"
	}
	log.Printf("Running integration test with endpoint %s", testEndpoint)
	opts := append(grpcHeaderChecker.CallOptions(), option.WithTokenSource(ts), option.WithEndpoint(testEndpoint))
	// Create InstanceAdmin and DatabaseAdmin clients.
	instanceAdmin, err = instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		log.Fatalf("cannot create instance databaseAdmin client: %v", err)
	}
	databaseAdmin, err = NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		log.Fatalf("cannot create databaseAdmin client: %v", err)
	}
	//TODO uncomment below if running with permissions to list instance configs
	// Get the list of supported instance configs for the project that is used
	// for the integration tests. The supported instance configs can differ per
	// project. The integration tests will use the first instance config that
	// is returned by Cloud Spanner. This will normally be the regional config
	// that is physically the closest to where the request is coming from.
	// configIterator := instanceAdmin.ListInstanceConfigs(ctx, &instancepb.ListInstanceConfigsRequest{
	// 	Parent: fmt.Sprintf("projects/%s", testProjectID),
	// })
	// config, err := configIterator.Next()
	// if err != nil {
	// 	log.Fatalf("Cannot get any instance configurations.\nPlease make sure the Cloud Spanner API is enabled for the test project.\nGet error: %v", err)
	// }
	// // Only delete the test instance if it was created.
	instanceDelete := false
	// if testInstanceName == "" {
	// 	testInstanceID := instanceNameSpace.New()
	// 	// Create a test instance to use for this test run.
	// 	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
	// 		Parent:     fmt.Sprintf("projects/%s", testProjectID),
	// 		InstanceId: testInstanceID,
	// 		Instance: &instancepb.Instance{
	// 			Config:      config.Name,
	// 			DisplayName: testInstanceID,
	// 			NodeCount:   1,
	// 		},
	// 	})
	// 	if err != nil {
	// 		log.Fatalf("could not create instance with id %s: %v", fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceID), err)
	// 	}
	// 	// Wait for the instance creation to finish.
	// 	i, err := op.Wait(ctx)
	// 	if err != nil {
	// 		log.Fatalf("waiting for instance creation to finish failed: %v", err)
	// 	}
	// 	if i.State != instancepb.Instance_READY {
	// 		log.Printf("instance state is not READY, it might be that the test instance will cause problems during tests. Got state %v\n", i.State)
	// 	}
	// 	instanceDelete = true
	// 	testInstanceName = testInstanceID
	// }

	return func() {
		// Delete this test instance.
		instanceName := fmt.Sprintf("projects/%v/instances/%v", testProjectID, testInstanceName)
		if instanceDelete {
			err := instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
				Name: instanceName,
			})
			if err != nil {
				log.Printf("failed to drop instance %s (error %v), might need a manual removal",
					instanceName, err)
			}
			// Delete other test instances that may be lingering around.
			cleanupInstances()
		} else {
			database := fmt.Sprintf("projects/%v/instances/%v/databases/%v", testProjectID, testInstanceName, testDatabaseName)
			err := databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: database})
			if err != nil {
				log.Printf("failed to drop database %s (error %v), might need a manual removal",
					database, err)
			}
		}
		databaseAdmin.Close()
		instanceAdmin.Close()
	}
}

// Prepare initializes Cloud Spanner testing DB and clients.
func prepareTestDatabase(ctx context.Context, t *testing.T, statements []string) {
	if databaseAdmin == nil {
		t.Skip("Integration tests skipped")
	}
	// Construct a unique test DB name if a database is not provided.
	dbName := testDatabaseName
	dbPath := fmt.Sprintf("projects/%v/instances/%v/databases/%v", testProjectID, testInstanceName, dbName)
	if dbName == "" {
		dbName= "ash_test123"
		//TODO use this instead of hardcoded DB
		// dbNameSpace.New()
		dbPath = fmt.Sprintf("projects/%v/instances/%v/databases/%v", testProjectID, testInstanceName, dbName)
		op, err := databaseAdmin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:          fmt.Sprintf("projects/%v/instances/%v", testProjectID, testInstanceName),
			CreateStatement: "CREATE DATABASE " + dbName,
			ExtraStatements: statements,
		})
		// Create database and tables.
		if err != nil {
			t.Fatalf("cannot create testing DB %v: %v", dbPath, err)
		}
		if _, err := op.Wait(ctx); err != nil {
			t.Fatalf("cannot create testing DB %v: %v", dbPath, err)
		}
		//update testDatabaseName if creation of new DB is successful
		testDatabaseName = dbName
	}

}

func cleanupInstances() {
	if instanceAdmin == nil {
		// Integration tests skipped.
		return
	}

	ctx := context.Background()
	parent := fmt.Sprintf("projects/%v", testProjectID)
	iter := instanceAdmin.ListInstances(ctx, &instancepb.ListInstancesRequest{
		Parent: parent,
		Filter: "name:gotest-",
	})
	expireAge := 24 * time.Hour

	for {
		inst, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		if instanceNameSpace.Older(inst.Name, expireAge) {
			log.Printf("Deleting instance %s", inst.Name)

			if err := instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{Name: inst.Name}); err != nil {
				log.Printf("failed to delete instance %s (error %v), might need a manual removal",
					inst.Name, err)
			}
		}
	}
}

func makeClient(ctx context.Context, t *testing.T) (*DatabaseAdminClient, func()) {
	prepareTestDatabase(ctx, t, singerDBStatements)
	dialOpt := option.WithGRPCDialOption(grpc.WithTimeout(5 * time.Second))
	epOpt := option.WithEndpoint(testEndpoint)
	adminClient, err := NewDatabaseAdminClient(ctx, dialOpt, epOpt)
	if err != nil {
		adminClient.Close()
		t.Fatalf("Connecting DB admin client: %v", err)
	}
	return adminClient, func() {
		adminClient.Close()
	}
}

func TestIntegrationCreateNewBackup(t *testing.T) {
	ctx := context.Background()
	instanceCleanup := initIntegrationTests()
	adminClient, cleanup := makeClient(ctx, t)
	backupID := uid.NewSpace("backupid", &uid.Options{Sep: '_', Short: true}).New()
	backupName := fmt.Sprintf("projects/%s/instances/%s/backups/%s", testProjectID, testInstanceName, backupID)
	fullDatabaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectID, testInstanceName, testDatabaseName)
	deleteBackupArgs := &databasepb.DeleteBackupRequest{}
	deleteBackupArgs.Name = backupName
	expires := time.Now().Add(time.Hour * 7)
	respLRO, err := adminClient.CreateNewBackup(ctx, backupID, fmt.Sprintf("projects/%v/instances/%v/databases/%v", testProjectID, testInstanceName, testDatabaseName), expires)
	if err != nil {
		t.Fatal(err)
	}

	_, err = respLRO.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	respMetadata, err := respLRO.Metadata()
	if respMetadata.Database != fullDatabaseName {
		adminClient.DeleteBackup(ctx, deleteBackupArgs)
		t.Fatal(err)
	}
	if respMetadata.Progress.ProgressPercent != 100 {
		adminClient.DeleteBackup(ctx, deleteBackupArgs)
		t.Fatal("Backup has not completed succesfully")
	}
	getBackupReq := &databasepb.GetBackupRequest{}
	getBackupReq.Name = backupName
	respCheck, err := adminClient.GetBackup(ctx, getBackupReq)
	if err != nil {
		adminClient.DeleteBackup(ctx, deleteBackupArgs)
		t.Fatal(fmt.Sprintf("Could not retrieve backup %s", backupName), err)
	}
	if respCheck.CreateTime == nil {
		adminClient.DeleteBackup(ctx, deleteBackupArgs)
		t.Fatal("Backup create time missing")
	}
	if respCheck.State != databasepb.Backup_READY {
		adminClient.DeleteBackup(ctx, deleteBackupArgs)
		t.Fatal("Backup not ready after request completion")
	}
	if respCheck.SizeBytes == 0 {
		adminClient.DeleteBackup(ctx, deleteBackupArgs)
		t.Fatal("Backup has 0 size ")
	}
	adminClient.DeleteBackup(ctx, deleteBackupArgs)
	cleanup()
	instanceCleanup()
}
