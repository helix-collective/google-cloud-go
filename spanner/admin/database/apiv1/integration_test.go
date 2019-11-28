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
	"cloud.google.com/go/spanner/spannertest"
	"github.com/googleapis/gax-go/v2"
	"testing"
	"time"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

var testDBFlag = flag.String("test_db", "", "Fully-qualified database name to test against; empty means use an in-memory fake.")
var endpointFlag = flag.String("api_endpoint", "", "Endpoint for grpc calls")

func dbName() string {
	if *testDBFlag != "" {
		return *testDBFlag
	}
	return "projects/fake-proj/instances/fake-instance/databases/fake-db"
}

func endpointURL() string {
	if *endpointFlag != "" {
		return *endpointFlag
	}
	return "spanner.googleapis.com:443"
}

func makeClient(t *testing.T) (*DatabaseAdminClient, func()) {
	// Despite the docs, this context is also used for auth,
	// so it needs to be long-lived.
	ctx := context.Background()

	if *testDBFlag != "" {
		t.Logf("Using real Spanner DB %s", *testDBFlag)

		dialOpt := option.WithGRPCDialOption(grpc.WithTimeout(5 * time.Second))
		epOpt := option.WithEndpoint(endpointURL())
		// client, err := spanner.NewClient(ctx, *testDBFlag, dialOpt,epOpt)
		// if err != nil {
		// 	t.Fatalf("Connecting to %s: %v", *testDBFlag, err)
		// }
		adminClient, err := NewDatabaseAdminClient(ctx, dialOpt, epOpt)
		if err != nil {
			adminClient.Close()
			t.Fatalf("Connecting DB admin client: %v", err)
		}
		return adminClient, func() { adminClient.Close() }
	}

	// Don't use SPANNER_EMULATOR_HOST because we need the raw connection for
	// the database admin client anyway.

	t.Logf("Using in-memory fake Spanner DB")
	srv, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("Starting in-memory fake: %v", err)
	}
	srv.SetLogger(t.Logf)
	dialCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, srv.Addr, grpc.WithInsecure())
	if err != nil {
		srv.Close()
		t.Fatalf("Dialing in-memory fake: %v", err)
	}
	adminClient, err := NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		srv.Close()
		t.Fatalf("Connecting to in-memory fake DB admin: %v", err)
	}
	return adminClient, func() {
		adminClient.Close()
		conn.Close()
		srv.Close()
	}
}

func TestCreateNewBackup(t *testing.T) {
	adminClient, cleanup := makeClient(t)
	defer cleanup()
	var ctx context.Context = context.Background()
	var backupID string = "backupid1355353273"
	project, instance, _, err := validDatabaseName(dbName())
	backupArgs := struct {
		backupID     string
		databasePath string
		expireTime   time.Time
		opts         []gax.CallOption
	}{
		backupID:     backupID,
		databasePath: dbName(),
		expireTime:   time.Now().Add(time.Hour * 7),
	}
	respLRO, err := adminClient.CreateNewBackup(ctx, backupArgs.backupID, backupArgs.databasePath, backupArgs.expireTime)
	if err != nil {
		t.Fatal(err)
	}
	_, err = respLRO.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	deleteBackupArgs := &databasepb.DeleteBackupRequest{}
	deleteBackupArgs.Name = fmt.Sprintf("projects/%s/instances/%s/backups/%s", project, instance, backupID)
	adminClient.DeleteBackup(ctx, deleteBackupArgs)
}
