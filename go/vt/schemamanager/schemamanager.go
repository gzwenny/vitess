// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"encoding/json"
	"fmt"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
)

// DataSourcer defines how the autoschema system get schema change commands
type DataSourcer interface {
	Open() error
	Read() ([]string, error)
	Close() error
}

// EventHandler defines callbacks for events happen during schema management
type EventHandler interface {
	OnDataSourcerReadSuccess([]string) error
	OnDataSourcerReadFail(error) error
	OnValidationSuccess([]string) error
	OnValidationFail(error) error
	OnExecutorComplete(*ExecuteResult) error
}

// Executor applies schema changes to underlying system
type Executor interface {
	Open() error
	Validate(sqls []string) error
	Execute(sqls []string) *ExecuteResult
	Close() error
}

// ExecuteResult contains information about schema management state
type ExecuteResult struct {
	FailedShards  []ShardWithError
	SuccessShards []ShardResult
	CurSqlIndex   int
	Sqls          []string
	ExecutorErr   string
}

// ShardWithError contains information why a shard failed to execute given sql
type ShardWithError struct {
	Shard string
	Err   string
}

// ShardResult contains sql execute information on a particula shard
type ShardResult struct {
	Shard  string
	Result *mproto.QueryResult
}

// Run schema changes on Vitess through VtGate
func Run(sourcer DataSourcer,
	exec Executor,
	handler EventHandler) error {
	if err := sourcer.Open(); err != nil {
		log.Errorf("failed to open data sourcer: %v", err)
		return err
	}
	defer sourcer.Close()
	sqls, err := sourcer.Read()
	if err != nil {
		log.Errorf("failed to read data from data sourcer: %v", err)
		handler.OnDataSourcerReadFail(err)
		return err
	}
	handler.OnDataSourcerReadSuccess(sqls)
	if err := exec.Open(); err != nil {
		log.Errorf("failed to open executor: %v", err)
		return err
	}
	defer exec.Close()
	if err := exec.Validate(sqls); err != nil {
		log.Errorf("validation fail: %v", err)
		handler.OnValidationFail(err)
		return err
	}
	handler.OnValidationSuccess(sqls)
	result := exec.Execute(sqls)
	handler.OnExecutorComplete(result)
	if result.ExecutorErr != "" || len(result.FailedShards) > 0 {
		out, _ := json.MarshalIndent(result, "", "  ")
		return fmt.Errorf("Schema change failed, ExecuteResult: %v\n", string(out))
	}
	return nil
}
