package main

import (
	"io"
	"io/fs"
)

type fakeInfo struct {
	fs.FileInfo

	name string
	size int64
}

func (fi *fakeInfo) Name() string {
	return fi.name
}

func (fi *fakeInfo) Size() int64 {
	return fi.size
}

type fakeFile struct {
	info *fakeInfo
}

func (ff *fakeFile) Stat() (fs.FileInfo, error) {
	return ff.info, nil
}

func (ff *fakeFile) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (ff *fakeFile) Close() error {
	return nil
}

type fakeFileHandler struct {
	filesByPackID map[string]*fakeFile
}

func (ffh *fakeFileHandler) Exists(packID string) bool {
	_, ok := ffh.filesByPackID[packID]

	return ok
}

func (ffh *fakeFileHandler) Get(packID string) (fs.File, error) {
	f, ok := ffh.filesByPackID[packID]

	if !ok {
		return nil, ErrNotFound
	}

	return f, nil
}

func (ffh *fakeFileHandler) Lookup(filename string) (string, error) {

	for id, ffile := range ffh.filesByPackID {
		if ffile.info.name == filename {
			return id, nil
		}
	}

	return "", ErrNotFound
}
