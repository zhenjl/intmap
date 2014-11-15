// Copyright (c) 2014 Dataence, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package intmap

import (
	"errors"

	"github.com/dataence/bithacks"
)

const (
	DefaultIntegerMapSize       = 10
	DefaultIntegerMapLoadFactor = 0.75
)

var (
	ErrNotFound error = errors.New("Node not found")
)

type intnode struct {
	key   uint32
	value interface{}
}

type stats struct {
	numElements uint32
	numSlots    uint32
	numFinds    uint32
	numSteps    uint32
	numResizes  uint32
}

// IntegerMap is an open address hash table with linear probing. It is specifically
// designed to use unsinged integer as keys.
type IntegerMap struct {
	nodes []intnode
	stats stats
}

func NewIntegerMap() *IntegerMap {
	return &IntegerMap{
		nodes: make([]intnode, bithacks.RoundUpPowerOfTwo32(DefaultIntegerMapSize)),
		stats: stats{
			numSlots: uint32(bithacks.RoundUpPowerOfTwo32(DefaultIntegerMapSize)),
		},
	}
}

func (this *IntegerMap) Size() int {
	return int(this.stats.numSlots)
}

func (this *IntegerMap) Len() int {
	return int(this.stats.numElements)
}

func (this *IntegerMap) Search(k uint32) (interface{}, error) {
	i, err := this.find(k)
	if err == ErrNotFound {
		return nil, ErrNotFound
	}

	return this.nodes[i].value, nil
}

func (this *IntegerMap) Insert(k uint32, v interface{}) error {
	if float64(this.stats.numElements)/float64(this.stats.numSlots) > DefaultIntegerMapLoadFactor {
		this.resize()
	}

	return this.insert(k, v)
}

func (this *IntegerMap) Delete(k uint32) (interface{}, error) {
	i, err := this.find(k)
	if err == ErrNotFound {
		return nil, ErrNotFound
	}

	var v interface{}
	v, this.nodes[i].value = this.nodes[i].value, nil
	this.stats.numElements--
	return v, nil
}

// find looks for the key in the array, if it's found, return that.
// if we hit an empty/unfilled slot, then the key is not there, so
// we return the empty slot and ErrNotFound.
// if we go thru the whole array and still can't find the key,
// it also means we didn't find any empty slots. In that case,
// return -1 and ErrNotFound.
func (this *IntegerMap) find(k uint32) (int, error) {
	this.stats.numFinds++

	idx := this.index(k)
	for i := range this.nodes[idx:] {
		j := int(idx) + i
		this.stats.numSteps++

		if this.nodes[j].key == k && this.nodes[j].value != nil {
			return j, nil
		} else if this.nodes[j].value == nil {
			return j, ErrNotFound
		}
	}

	for j := range this.nodes[:idx] {
		this.stats.numSteps++

		if this.nodes[j].key == k && this.nodes[j].value != nil {
			return j, nil
		} else if this.nodes[j].value == nil {
			return j, ErrNotFound
		}
	}

	return -1, ErrNotFound
}

func (this *IntegerMap) insert(k uint32, v interface{}) error {
	i, err := this.find(k)
	if err == ErrNotFound {
		if i != -1 {
			// means we didn't find the key but did find an empty slot
			// so insert this as a new node
			this.nodes[i].key, this.nodes[i].value = k, v
			this.stats.numElements++
			return nil
		} else {
			// technicall we should never get here (no empty slots) since
			// we resize when the load factor is reached
			panic("Hashmap should never be full")
		}
	}

	this.nodes[i].value = v
	return nil
}

func (this *IntegerMap) resize() {
	newlen := this.stats.numSlots * 2
	nodes := make([]intnode, newlen)

	this.nodes, nodes = nodes, this.nodes
	this.stats.numElements = 0
	this.stats.numSlots = newlen

	for _, n := range nodes {
		if n.value != nil {
			this.insert(n.key, n.value)
		}
	}

	this.stats.numResizes++
}

func (this *IntegerMap) index(k uint32) uint32 {
	return bithacks.Fmix32(k) & (this.stats.numSlots - 1)
}
