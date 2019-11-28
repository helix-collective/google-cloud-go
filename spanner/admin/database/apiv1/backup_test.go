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
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	pbt "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/googleapis/gax-go/v2"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	status "google.golang.org/genproto/googleapis/rpc/status"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
	"reflect"
	"testing"
	"time"
)

func Test_validDatabaseName(t *testing.T) {
	type args struct {
		db string
	}
	tests := []struct {
		name         string
		args         args
		wantProject  string
		wantInstance string
		wantDatabase string
		wantErr      bool
	}{
		{name: "correct database path",
			args: args{
				db: "projects/spanner-cloud-test/instances/fooinstance/databases/foodb",
			},
			wantProject:  "spanner-cloud-test",
			wantInstance: "fooinstance",
			wantDatabase: "foodb",
			wantErr:      false,
		},
		{name: "incorrect database path",
			args: args{
				db: "project/instances/databases/foodb",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProject, gotInstance, gotDatabase, err := validDatabaseName(tt.args.db)
			if (err != nil) != tt.wantErr {
				t.Errorf("validDatabaseName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotProject != tt.wantProject {
				t.Errorf("validDatabaseName() gotProject = %v, want %v", gotProject, tt.wantProject)
			}
			if gotInstance != tt.wantInstance {
				t.Errorf("validDatabaseName() gotInstance = %v, want %v", gotInstance, tt.wantInstance)
			}
			if gotDatabase != tt.wantDatabase {
				t.Errorf("validDatabaseName() gotDatabase = %v, want %v", gotDatabase, tt.wantDatabase)
			}

		})
	}
}

func Test_timestampProto(t *testing.T) {
	type args struct {
		t time.Time
	}
	tests := []struct {
		name string
		args args
		want *pbt.Timestamp
	}{
		{name: "test Unix 0 time",
			args: args{time.Unix(0, 0)},
			want: &pbt.Timestamp{}},
		{name: "test Unix positive time",
			args: args{time.Unix(1136239445, 12345)},
			want: &pbt.Timestamp{Seconds: 1136239445, Nanos: 12345},
		},
		{name: "test Unix negative time",
			args: args{time.Unix(-1000, 12345)},
			want: &pbt.Timestamp{Seconds: -1000, Nanos: 12345}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := timestampProto(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("timestampProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatabaseAdminClient_CreateNewBackup(t *testing.T) {
	var name string = "name3373707"
	var database string = "database1789464955"
	var sizeBytes int64 = 1796325715
	var expectedResponse = &databasepb.Backup{
		Name:      name,
		Database:  database,
		SizeBytes: sizeBytes,
	}
	mockDatabaseAdmin.err = nil
	mockDatabaseAdmin.reqs = nil
	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockDatabaseAdmin.resps = append(mockDatabaseAdmin.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})
	var formattedDatabasePath string = fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", database)
	var backupID string = "backupId1355353272"
	c, err := NewDatabaseAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}
	backupArgs := struct {
		ctx          context.Context
		backupID     string
		databasePath string
		expireTime   time.Time
		opts         []gax.CallOption
	}{
		ctx:          context.Background(),
		backupID:     backupID,
		databasePath: formattedDatabasePath,
		expireTime:   time.Now().Add(time.Hour * 7),
	}
	respLRO, err := c.CreateNewBackup(backupArgs.ctx, backupArgs.backupID, backupArgs.databasePath, backupArgs.expireTime)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDatabaseAdminCreateNewBackupError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDatabaseAdmin.err = nil
	mockDatabaseAdmin.resps = append(mockDatabaseAdmin.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var backupId string = "backupId1355353272"
	var backup *databasepb.Backup = &databasepb.Backup{}
	var request = &databasepb.CreateBackupRequest{
		Parent:   formattedParent,
		BackupId: backupId,
		Backup:   backup,
	}
	c, err := NewDatabaseAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.CreateBackup(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
