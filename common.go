/*
 * Copyright (C) 2018 The ontology Authors
 * This file is part of The ontology library.
 *
 * The ontology is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ontology is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with The ontology.  If not, see <http://www.gnu.org/licenses/>.
 */

package leveldb

import (
	"errors"

	ec "github.com/ont-project/ontology-framework/common/config"
	"github.com/ont-project/ontology-framework/common/di"
	"github.com/ont-project/ontology-framework/core"
	"github.com/ont-project/ontology-framework/core/channel"
	"github.com/ont-project/ontology/runtime"
)

type StorageModule struct {
	runtime.RuntimeModule

	db *LevelDBStore
}

type StorageConfig struct {
	ec.Config
	channel.ChannelSetConfig

	DBPath string `json:"db_path"`
}

var storageExtra ec.Config = ec.Config{Magic: core.MagicStorage, Version: 1}

func init() {
	di.ConfigMap.Register(ec.GetExtraId(storageExtra), &StorageConfig{})
	di.EngineMap.Register(ec.GetExtraId(storageExtra), &StorageModule{})
}

func (self *StorageModule) Init() error {
	if err := self.RuntimeModule.Init(); err != nil {
		return err
	}
	conf := self.GetModule().Config.(*StorageConfig)

	db, err := NewLevelDBStore(conf.DBPath)
	if err != nil {
		return err
	}
	self.db = db
	return nil
}

func (self *StorageModule) NewStub() (channel.Stub, error) {
	return &core.StorageStub{}, nil
}

func (self *StorageModule) ExecuteGo(stub channel.Stub, cmd channel.Command, param ...interface{}) (interface{}, error) {
	switch cmd {
	case core.Put:
		data := param[0]
		data2 := param[1]
		return nil, self.Put(data.([]byte), data2.([]byte))
	case core.Get:
		data := param[0]
		return self.Get(data.([]byte))
	case core.Has:
		data := param[0]
		return self.Has(data.([]byte))
	case core.Delete:
		data := param[0]
		return nil, self.Delete(data.([]byte))
	case core.NewBatch:
		return nil, self.NewBatch()
	case core.BatchPut:
		data := param[0]
		data2 := param[1]
		return nil, self.BatchPut(data.([]byte), data2.([]byte))
	case core.BatchDelete:
		data := param[0]
		return nil, self.BatchDelete(data.([]byte))
	case core.BatchCommit:
		return nil, self.BatchCommit()
	case core.Close:
		return nil, self.Close()
	case core.NewIterator:
		data := param[0]
		return self.NewIterator(data.([]byte)), nil
	default:
		return nil, errors.New("unknown command")
	}
}

func (self *StorageModule) Put(key []byte, value []byte) error {
	return self.db.Put(key, value)
}
func (self *StorageModule) Get(key []byte) ([]byte, error) {
	r, err := self.db.Get(key)
	if err == core.ErrNotFound {
		err = nil
	}
	return r, err
}
func (self *StorageModule) Has(key []byte) (bool, error) {
	return self.db.Has(key)
}
func (self *StorageModule) Delete(key []byte) error {
	return self.db.Delete(key)
}
func (self *StorageModule) NewBatch() error {
	self.db.NewBatch()
	return nil
}
func (self *StorageModule) BatchPut(key []byte, value []byte) error {
	self.db.BatchPut(key, value)
	return nil
}
func (self *StorageModule) BatchDelete(key []byte) error {
	self.db.BatchDelete(key)
	return nil
}
func (self *StorageModule) BatchCommit() error {
	return self.db.BatchCommit()
}
func (self *StorageModule) Close() error {
	return self.db.Close()
}
func (self *StorageModule) NewIterator(prefix []byte) core.Iterator {
	return self.db.NewIterator(prefix)
}
