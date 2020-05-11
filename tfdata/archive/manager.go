// Package archive contains tools for transition between TAR files and SampleReader
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package archive

type (
	Record struct {
		Name    string
		Members map[string][]byte
	}

	RecordsManager interface {
		StoreRecord(name string, record *Record)
		UpdateRecord(name string, member string, value []byte)
		GetRecord(name string) *Record
		GetRecords() map[string]*Record
		DeleteRecord(name string)
		Len() int
	}

	RecordsDefaultManager struct {
		Records map[string]*Record
	}
)

func NewRecord(name string) *Record {
	return &Record{Name: name, Members: make(map[string][]byte)}
}

func (r *Record) SetMember(name string, value []byte) {
	r.Members[name] = value
}

func (r *Record) SameMembers(record *Record) bool {
	if len(r.Members) != len(record.Members) {
		return false
	}
	for k := range r.Members {
		if _, ok := record.Members[k]; !ok {
			return false
		}
	}
	return true
}

func NewRecordsManager() *RecordsDefaultManager {
	return &RecordsDefaultManager{Records: make(map[string]*Record)}
}

func (m *RecordsDefaultManager) Len() int {
	return len(m.Records)
}

func (m *RecordsDefaultManager) StoreRecord(name string, r *Record) {
	m.Records[name] = r
}

func (m *RecordsDefaultManager) UpdateRecord(name, member string, value []byte) {
	if m.Records[name] == nil {
		m.Records[name] = NewRecord(name)
	}

	m.Records[name].Members[member] = value
}

func (m *RecordsDefaultManager) GetRecord(name string) *Record {
	return m.Records[name]
}

func (m *RecordsDefaultManager) GetRecords() map[string]*Record {
	return m.Records
}

func (m *RecordsDefaultManager) DeleteRecord(name string) {
	delete(m.Records, name)
}
