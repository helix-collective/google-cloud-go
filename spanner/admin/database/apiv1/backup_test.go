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
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	status "google.golang.org/genproto/googleapis/rpc/status"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

func TestDatabaseAdminClient_CreateNewBackup(t *testing.T) {
	name := "name3373707"
	database := "database1789464955"
	sizeBytes := int64(1796325715123)
	expectedResponse := &databasepb.Backup{
		Name:      name,
		Database:  database,
		SizeBytes: sizeBytes,
	}
	mockDatabaseAdmin.err = nil
	mockDatabaseAdmin.reqs = nil
	formattedDatabasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", database)
	backupID := "backupid1355353272"
	expires := time.Now().Add(time.Hour * 7)
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
	respLRO, err := c.CreateNewBackup(ctx, backupID, formattedDatabasePath, expires)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	//TODO add test for expire time conversion to pb
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
	formattedDatabasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "database1789464955")
	backupID := "backupid1355353272"
	expires := time.Now().Add(time.Hour * 7)
	ctx := context.Background()
	c, err := NewDatabaseAdminClient(ctx, clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.CreateNewBackup(ctx, backupID, formattedDatabasePath, expires)
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
