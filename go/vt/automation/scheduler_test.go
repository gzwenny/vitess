// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	context "golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

func TestSchedulerImmediateShutdown(t *testing.T) {
	// Make sure that the scheduler shuts down cleanly when it was instantiated, but not started with Run().
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler.ShutdownAndWait()
}

func TestEnqueueSingleTask(t *testing.T) {
	enqueueClusterOperationAndCheckOutput(t, "TestingEchoTask", "echoed text")
}

func TestEnqueueEmittingTask(t *testing.T) {
	enqueueClusterOperationAndCheckOutput(t, "TestingEmitEchoTask", "echoed text from emitted task")
}

func enqueueClusterOperationAndCheckOutput(t *testing.T, taskName string, expectedOutput string) {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler.setTaskCreator(testingTaskCreator)
	scheduler.registerClusterOperation("TestingEchoTask")
	scheduler.registerClusterOperation("TestingEmitEchoTask")

	scheduler.Run()

	enqueueRequest := &pb.EnqueueClusterOperationRequest{
		Name: taskName,
		Parameters: map[string]string{
			"echo_text": expectedOutput,
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.TODO(), enqueueRequest)
	if err != nil {
		t.Fatalf("Failed to start cluster operation. Request: %v Error: %v", enqueueRequest, err)
	}

	waitForClusterOperation(t, scheduler, enqueueResponse.Id, expectedOutput, "")

	scheduler.ShutdownAndWait()
}

func waitForClusterOperation(t *testing.T, scheduler *Scheduler, id string, expectedOutputLastTask string, expectedErrorLastTask string) *pb.ClusterOperation {
	if expectedOutputLastTask == "" && expectedErrorLastTask == "" {
		t.Fatal("Error in test: Cannot wait for an operation where both output and error are expected to be empty.")
	}

	getDetailsRequest := &pb.GetClusterOperationDetailsRequest{
		Id: id,
	}

	for {
		getDetailsResponse, err := scheduler.GetClusterOperationDetails(context.TODO(), getDetailsRequest)
		if err != nil {
			t.Fatalf("Failed to get details for cluster operation. Request: %v Error: %v", getDetailsRequest, err)
		}
		if getDetailsResponse.ClusterOp.State == pb.ClusterOperationState_CLUSTER_OPERATION_DONE {
			tc := getDetailsResponse.ClusterOp.SerialTasks
			lastTc := tc[len(tc)-1]
			if expectedOutputLastTask != "" {
				if lastTc.ParallelTasks[len(lastTc.ParallelTasks)-1].Output != expectedOutputLastTask {
					t.Fatalf("ClusterOperation finished but did not return expected output. want: %v Full ClusterOperation details: %v", expectedOutputLastTask, proto.MarshalTextString(getDetailsResponse.ClusterOp))
				}
			}
			if expectedErrorLastTask != "" {
				if lastTc.ParallelTasks[len(lastTc.ParallelTasks)-1].Error != expectedErrorLastTask {
					t.Fatalf("ClusterOperation finished but did not return expected error. Full ClusterOperation details: %v", getDetailsResponse.ClusterOp)
				}
			}
			return getDetailsResponse.ClusterOp
		}

		t.Logf("Waiting for clusterOp: %v", getDetailsResponse.ClusterOp)
		time.Sleep(5 * time.Millisecond)
	}
}

func TestEnqueueFailsDueToMissingParameter(t *testing.T) {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler.setTaskCreator(testingTaskCreator)
	scheduler.registerClusterOperation("TestingEchoTask")

	scheduler.Run()

	enqueueRequest := &pb.EnqueueClusterOperationRequest{
		Name: "TestingEchoTask",
		Parameters: map[string]string{
			"unrelevant-parameter": "value",
		},
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.TODO(), enqueueRequest)

	if err == nil {
		t.Fatalf("Scheduler should have failed to start cluster operation because not all required parameters were provided. Request: %v Error: %v Response: %v", enqueueRequest, err, enqueueResponse)
	}
	want := "Parameter echo_text is required, but not provided"
	if err.Error() != want {
		t.Fatalf("Wrong error message. got: '%v' want: '%v'", err, want)
	}

	scheduler.ShutdownAndWait()
}

func TestFailedTaskFailsClusterOperation(t *testing.T) {
	scheduler, err := NewScheduler()
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler.setTaskCreator(testingTaskCreator)
	scheduler.registerClusterOperation("TestingFailTask")

	scheduler.Run()

	enqueueRequest := &pb.EnqueueClusterOperationRequest{
		Name: "TestingFailTask",
	}
	enqueueResponse, err := scheduler.EnqueueClusterOperation(context.TODO(), enqueueRequest)

	waitForClusterOperation(t, scheduler, enqueueResponse.Id, "something went wrong", "full error message")

	scheduler.ShutdownAndWait()
}