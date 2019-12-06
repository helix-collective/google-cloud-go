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
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	status "google.golang.org/genproto/googleapis/rpc/status"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

func TestDatabaseAdminClient_CreateNewBackup(t *testing.T) {
	backupName := "some-backup"
	databaseName := "some-database"
	instancePath := "projects/some-project/instances/some-instance"
	databasePath := instancePath + "/databases/" + databaseName
	backupPath := instancePath + "/backups/" + backupName
	expectedRequest := &databasepb.CreateBackupRequest{
		Parent:   instancePath,
		BackupId: backupName,
		Backup: &databasepb.Backup{
			Database: databasePath,
			ExpireTime: &timestamp.Timestamp{
				Seconds: 221688000,
				Nanos:   500,
			},
		},
	}
	expectedResponse := &databasepb.Backup{
		Name:      backupPath,
		Database:  databasePath,
		SizeBytes: 1796325715123,
	}
	mockDatabaseAdmin.err = nil
	mockDatabaseAdmin.reqs = nil

	ctx := context.Background()
	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockDatabaseAdmin.resps = append(mockDatabaseAdmin.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})
	c, err := NewDatabaseAdminClient(ctx, clientOpt)
	if err != nil {
		t.Fatal(err)
	}
	respLRO, err := c.CreateNewBackup(ctx, backupName, databasePath, time.Unix(221688000, 500))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := expectedRequest, mockDatabaseAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}
	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDatabaseAdminCreateNewBackupError(t *testing.T) {
	wantErr := codes.PermissionDenied
	mockDatabaseAdmin.err = nil
	mockDatabaseAdmin.resps = append(mockDatabaseAdmin.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(wantErr),
				Message: "test error",
			},
		},
	})
	databaseName := "some-database"
	backupName := "some-backup"
	instancePath := "projects/some-project/instances/some-instance"
	databasePath := instancePath + "/databases/" + databaseName
	// Minimum expiry time is 6 hours
	expires := time.Now().Add(time.Hour * 7)
	ctx := context.Background()
	c, err := NewDatabaseAdminClient(ctx, clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.CreateNewBackup(ctx, backupName, databasePath, expires)
	if err != nil {
		t.Fatal(err)
	}
	_, reqerr := respLRO.Wait(ctx)
	st, ok := gstatus.FromError(reqerr)
	if !ok {
		t.Fatalf("got error %v, expected grpc error", reqerr)
	}
	if st.Code() != wantErr {
		t.Fatalf("got error code %q, want %q", st.Code(), wantErr)
	}
}
