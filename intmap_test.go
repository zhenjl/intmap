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
	"testing"

	"github.com/surge/assert"
	"github.com/surge/glog"
)

func TestIntegerMap(t *testing.T) {
	m := NewIntegerMap()

	_, err := m.Search(1)

	assert.NotNil(t, true, err, "Search should have returned error")

	_, err = m.Delete(1)

	assert.NotNil(t, true, err, "Delete should have returned error")

	err = m.Insert(1, 100)

	assert.Nil(t, true, err, "Insert should NOT have returned error")
	assert.Equal(t, true, m.Len(), 1)

	v, err := m.Search(1)

	assert.Nil(t, true, err, "Search should NOT have returned error")
	assert.Equal(t, true, v.(int), 100)

	err = m.Insert(1, 200)

	assert.Nil(t, true, err, "Insert should NOT have returned error")
	assert.Equal(t, true, m.Len(), 1)

	v, err = m.Search(1)

	assert.Nil(t, true, err, "Search should NOT have returned error")
	assert.Equal(t, true, v.(int), 200)

	err = m.Insert(100000, 100000)

	assert.Nil(t, true, err, "Insert should NOT have returned error")
	assert.Equal(t, true, m.Len(), 2)

	v, err = m.Delete(1)

	assert.Nil(t, true, err, "Delete should NOT have returned error")
	assert.Equal(t, true, v.(int), 200)
	assert.Equal(t, true, m.Len(), 1)

	v, err = m.Delete(1)

	assert.NotNil(t, true, err, "Delete should have returned error")
	assert.Equal(t, true, m.Len(), 1)
}

func TestIntegerMapResize(t *testing.T) {
	for _, n := range []int{10, 100, 1000, 10000, 100000} {
		m := NewIntegerMap()

		for i := 0; i < n; i++ {
			m.Insert(uint32(i), i)
		}

		assert.Equal(t, true, m.Len(), n)

		v, err := m.Search(uint32(n - 1))

		assert.Nil(t, true, err, "Search should NOT have returned error")
		assert.Equal(t, true, v.(int), n-1)

		glog.Infof("Average steps taken = %.2f", float64(m.stats.numSteps)/float64(m.stats.numFinds))
	}
}

func integerInsert(b *testing.B) *IntegerMap {
	m := NewIntegerMap()

	for i := 0; i < b.N; i++ {
		m.Insert(uint32(i), i)
	}

	return m
}

func BenchmarkIntegerInsert(b *testing.B) {
	integerInsert(b)
}

func mapInsert(b *testing.B) map[int]interface{} {
	m := make(map[int]interface{}, DefaultIntegerMapSize)

	for i := 0; i < b.N; i++ {
		m[i] = i
	}

	return m
}

func BenchmarkMapInsert(b *testing.B) {
	mapInsert(b)
}

func BenchmarkMapSearch(b *testing.B) {
	m := mapInsert(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		a := m[i]
		_ = a
	}
}

func BenchmarkIntegerSearch(b *testing.B) {
	m := integerInsert(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Search(uint32(i))
	}
}
