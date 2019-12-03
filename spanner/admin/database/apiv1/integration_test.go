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
)

var (
	// testProjectID specifies the project used for testing. It can be changed
	// by setting environment variable GCLOUD_TESTS_GOLANG_PROJECT_ID.
	testProjectID    = testutil.ProjID()
	testInstanceName = os.Getenv("GCLOUD_TESTS_GOLANG_INSTANCE_NAME")
	testEndpoint     = os.Getenv("GCLOUD_TESTS_GOLANG_ENDPOINT")

	dbNameSpace       = uid.NewSpace("gotest", &uid.Options{Sep: '_', Short: true})
	instanceNameSpace = uid.NewSpace("gotest", &uid.Options{Sep: '-', Short: true})
	backupNameSpace   = uid.NewSpace("gotest", &uid.Options{Sep: '_', Short: true})

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

	// Check if a specific endpoint is set for the integration test
	opts := append(grpcHeaderChecker.CallOptions(), option.WithTokenSource(ts))
	if testEndpoint != "" {
		log.Printf("Running integration test with endpoint %s", testEndpoint)
		opts = append(opts, option.WithEndpoint(testEndpoint))
	}

	// Create InstanceAdmin and DatabaseAdmin clients.
	instanceAdmin, err = instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		log.Fatalf("cannot create instance databaseAdmin client: %v", err)
	}
	databaseAdmin, err = NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		log.Fatalf("cannot create databaseAdmin client: %v", err)
	}

	// If a specific instance was selected for testing, use that.  Otherwise create a new instance for testing and
	// tear it down after the test.
	createInstanceForTest := testInstanceName == ""
	if createInstanceForTest {
		testInstanceName = instanceNameSpace.New()

		// Get the list of supported instance configs for the project that is used
		// for the integration tests. The supported instance configs can differ per
		// project. The integration tests will use the first instance config that
		// is returned by Cloud Spanner. This will normally be the regional config
		// that is physically the closest to where the request is coming from.
		configIterator := instanceAdmin.ListInstanceConfigs(ctx, &instancepb.ListInstanceConfigsRequest{
			Parent: fmt.Sprintf("projects/%s", testProjectID),
		})
		config, err := configIterator.Next()
		if err != nil {
			log.Fatalf("Cannot get any instance configurations.\nPlease make sure the Cloud Spanner API is enabled for the test project.\nGet error: %v", err)
		}

		// Create a test instance to use for this test run.
		op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
			Parent:     fmt.Sprintf("projects/%s", testProjectID),
			InstanceId: testInstanceName,
			Instance: &instancepb.Instance{
				Config:      config.Name,
				DisplayName: testInstanceName,
				NodeCount:   1,
			},
		})
		if err != nil {
			log.Fatalf("could not create instance with id %s: %v", fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceName), err)
		}
		// Wait for the instance creation to finish.
		i, err := op.Wait(ctx)
		if err != nil {
			log.Fatalf("waiting for instance creation to finish failed: %v", err)
		}
		if i.State != instancepb.Instance_READY {
			log.Printf("instance state is not READY, it might be that the test instance will cause problems during tests. Got state %v\n", i.State)
		}
	}

	return func() {
		if createInstanceForTest {
			err := instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
				Name: testInstanceName,
			})
			if err != nil {
				log.Printf("failed to drop instance %s (error %v), might need a manual removal",
					testInstanceName, err)
			}
			// Delete other test instances that may be lingering around.
			cleanupInstances()
		}

		databaseAdmin.Close()
		instanceAdmin.Close()
	}
}

// Prepare initializes Cloud Spanner testing DB and clients.
func prepareIntegrationTest(ctx context.Context, t *testing.T) (string, func()) {
	if databaseAdmin == nil {
		t.Skip("Integration tests skipped")
	}
	// Construct a unique test DB name.
	dbName := dbNameSpace.New()

	dbPath := fmt.Sprintf("projects/%v/instances/%v/databases/%v", testProjectID, testInstanceName, dbName)
	// Create database and tables.
	op, err := databaseAdmin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%v/instances/%v", testProjectID, testInstanceName),
		CreateStatement: "CREATE DATABASE " + dbName,
		ExtraStatements: singerDBStatements,
	})
	if err != nil {
		t.Fatalf("cannot create testing DB %v: %v", dbPath, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("cannot create testing DB %v: %v", dbPath, err)
	}

	return dbPath, func() {
		err := databaseAdmin.DropDatabase(ctx, &adminpb.DropDatabaseRequest{
			Database: dbPath,
		})
		if err != nil {
			t.Fatalf("cannot drop testing DB %v: %v", dbPath, err)
		}
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

func TestIntegrationCreateNewBackup(t *testing.T) {
	ctx := context.Background()
	instanceCleanup := initIntegrationTests()
	defer instanceCleanup()
	testDatabaseName, cleanup := prepareIntegrationTest(ctx, t)
	defer cleanup()

	backupID := backupNameSpace.New()
	backupName := fmt.Sprintf("projects/%s/instances/%s/backups/%s", testProjectID, testInstanceName, backupID)
	expires := time.Now().Add(time.Hour * 7)
	respLRO, err := databaseAdmin.CreateNewBackup(ctx, backupID, testDatabaseName, expires)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		deleteBackupArgs := &databasepb.DeleteBackupRequest{}
		deleteBackupArgs.Name = backupName
		err := databaseAdmin.DeleteBackup(ctx, deleteBackupArgs)
		if err != nil {
			t.Logf("Error deleting backup: %v", err)
		}
	}()

	_, err = respLRO.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	respMetadata, err := respLRO.Metadata()
	if err != nil {
		t.Fatalf("Error getting metadata from backup operation: %v", err)
	}
	if respMetadata.Database != testDatabaseName {
		t.Fatalf("Backup has wrong database name, expected %s but got %s", testDatabaseName, respMetadata.Database)
	}
	if respMetadata.Progress.ProgressPercent != 100 {
		t.Fatal("Backup has not completed successfully")
	}
	getBackupReq := &databasepb.GetBackupRequest{}
	getBackupReq.Name = backupName
	respCheck, err := databaseAdmin.GetBackup(ctx, getBackupReq)
	if err != nil {
		t.Fatalf("Could not retrieve backup %s: %v", backupName, err)
	}
	if respCheck.CreateTime == nil {
		t.Fatal("Backup create time missing")
	}
	if respCheck.State != databasepb.Backup_READY {
		t.Fatal("Backup not ready after request completion")
	}
	if respCheck.SizeBytes == 0 {
		t.Fatal("Backup has 0 size")
	}
}
