package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	listPack     = "list"
	listFilename = "fulminata.txt"
)

var ErrNotFound = errors.New("pack not found")

type PackInfo struct {
	PackID string
	Name   string
	Size   int64
}

type fsHandler struct {
	fsAccess   fs.FS
	pool       *portsPool
	lookupID   map[string]PackInfo
	lookupPack map[PackInfo]string

	// count
	countBufferMu             sync.Mutex
	countBuffer               map[string]int
	downloadsCountBackingFile string
}

func MustNewFSHandler(rootSavePath, counterFile string, pool *portsPool) *fsHandler {
	fsAccess := os.DirFS(rootSavePath)
	lookupID, lookupPack := mustBuildLookups(fsAccess)

	fh := &fsHandler{
		fsAccess:                  fsAccess,
		pool:                      pool,
		lookupID:                  lookupID,
		lookupPack:                lookupPack,
		countBuffer:               make(map[string]int),
		downloadsCountBackingFile: counterFile,
	}

	if err := fh.loadCounts(); err != nil {
		panic(err)
	}

	persistanceTicker := time.NewTicker(4 * time.Minute)

	go func() {
		for {
			select {
			case <-persistanceTicker.C:
				if err := fh.persistCounts(); err != nil {
					fmt.Println(err)
				} else {
					fmt.Println("persisted counts")
				}
			}
		}
	}()

	return fh
}

func filterFileName(filename string) bool {
	for _, suffix := range []string{
		".ttf",
		".md5",
		".jpg",
		".png",
		".txt",
		".json",
	} {
		if strings.HasSuffix(filename, suffix) {
			return true
		}
	}

	return false
}

func mustBuildLookups(access fs.FS) (map[string]PackInfo, map[PackInfo]string) {
	count := 1
	idLookup := make(map[string]PackInfo)
	packLookup := make(map[PackInfo]string)

	err := fs.WalkDir(
		access,
		".",
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

			if filterFileName(d.Name()) {
				return nil
			}

			fileInfo, err := d.Info()

			if err != nil {
				return err
			}

			packID := fmt.Sprintf("%d", count)
			info := PackInfo{
				PackID: packID,
				Name:   d.Name(),
				Size:   fileInfo.Size(),
			}

			idLookup[packID] = info
			packLookup[info] = path
			count++

			return err
		},
	)

	if err != nil {
		panic(err)
	}

	return idLookup, packLookup
}

func (fh *fsHandler) Exists(packID string) bool {
	packID = trimPackID(packID)

	if strings.ToLower(packID) == listPack {
		return true
	}

	_, ok := fh.lookupID[packID]

	return ok
}

func trimPackID(id string) string {
	return strings.TrimFunc(
		id,
		func(r rune) bool {
			return r == '\x01' || r == '#' || r == ' '
		},
	)
}

func (fh *fsHandler) Get(packID string) (fs.File, error) {
	packID = trimPackID(packID)

	if strings.ToLower(packID) == listPack {
		return fh.list()
	}

	info, ok := fh.lookupID[packID]

	if !ok {
		return nil, ErrNotFound
	}

	packPath, ok := fh.lookupPack[info]

	if !ok {
		return nil, ErrNotFound
	}

	fh.countBufferMu.Lock()
	fh.countBuffer[packPath]++
	fh.countBufferMu.Unlock()

	return fh.fsAccess.Open(packPath)
}

func (fh *fsHandler) Lookup(filename string) (string, error) {
	if filename == listFilename {
		return listPack, nil
	}

	for id, info := range fh.lookupID {
		if info.Name == filename {
			return id, nil
		}
	}

	return "", ErrNotFound
}

func (fh *fsHandler) list() (fs.File, error) {
	infos := slices.Collect(maps.Values(fh.lookupID))

	slices.SortFunc(infos, func(l, r PackInfo) int {
		intL, _ := strconv.Atoi(strings.TrimPrefix(l.PackID, "#"))
		intR, _ := strconv.Atoi(strings.TrimPrefix(r.PackID, "#"))

		return cmp.Compare(intL, intR)
	})

	b := bytes.Buffer{}

	b.Write([]byte("** Fulminata is an experimental bot I wrote myself, please expect some weird stuff **\n"))
	b.Write(fmt.Appendf([]byte{}, "** %d packs, %d of %d slots open **\n", len(infos), fh.pool.Free(), fh.pool.Total()))

	fh.countBufferMu.Lock()
	defer fh.countBufferMu.Unlock()

	for _, pack := range infos {
		path := fh.lookupPack[pack]
		count := fh.countBuffer[path]

		b.Write(fmt.Appendf([]byte{}, "#%s %dx [%s] %s\n", pack.PackID, count, format(pack.Size), pack.Name))
	}

	reader := bytes.NewReader(b.Bytes())

	return inMemoryFile{
		reader: reader,
		info: inMemoryFileInfo{
			name: listFilename,
			size: int64(reader.Len()),
		},
	}, nil
}

func (fh *fsHandler) loadCounts() error {
	countFile, err := os.Open(fh.downloadsCountBackingFile)

	switch {
	case err == nil:
	case os.IsNotExist(err):
		fh.countBuffer = make(map[string]int)

		return nil
	default:
		return err
	}

	defer countFile.Close()

	var counts map[string]int

	if err := json.NewDecoder(countFile).Decode(&counts); err != nil {
		return err
	}

	fh.countBufferMu.Lock()
	fh.countBuffer = counts
	fh.countBufferMu.Unlock()

	return nil
}

func (fh *fsHandler) persistCounts() error {
	fh.countBufferMu.Lock()
	snapshot := maps.Clone(fh.countBuffer)
	fh.countBufferMu.Unlock()

	countFile, err := os.OpenFile(fh.downloadsCountBackingFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)

	defer countFile.Close()

	if err != nil {
		return err
	}

	return json.NewEncoder(countFile).Encode(snapshot)
}

func format(s int64) string {
	sizes := []string{"B", "K", "M", "G"}
	return humanateBytes(s, 1000, sizes)
}

func logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

func humanateBytes(s int64, base float64, sizes []string) string {
	if s < 10 {
		return fmt.Sprintf("%dB", s)
	}
	e := math.Floor(logn(float64(s), base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}

	return fmt.Sprintf(f, val, suffix)
}

type inMemoryFile struct {
	reader *bytes.Reader
	info   inMemoryFileInfo
}

type inMemoryFileInfo struct {
	fs.FileInfo
	name string
	size int64
}

func (imfi inMemoryFileInfo) Name() string { return imfi.name }
func (imfi inMemoryFileInfo) Size() int64  { return imfi.size }

func (imf inMemoryFile) Stat() (fs.FileInfo, error) {
	return imf.info, nil
}

func (imf inMemoryFile) Read(b []byte) (int, error) {
	return imf.reader.Read(b)
}

func (imf inMemoryFile) ReadAt(b []byte, off int64) (int, error) {
	return imf.reader.ReadAt(b, off)
}

func (imf inMemoryFile) Close() error { return nil }
