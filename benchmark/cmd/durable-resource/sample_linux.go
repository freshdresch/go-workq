package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type fileSample struct {
	Exists    bool  `json:"exists"`
	Length    int64 `json:"length_bytes"`
	Allocated int64 `json:"allocated_bytes"`
}

type resourceSample struct {
	At                 time.Time  `json:"at"`
	RSS                uint64     `json:"rss_bytes"`
	HeapAlloc          uint64     `json:"go_heap_alloc_bytes"`
	HeapInuse          uint64     `json:"go_heap_inuse_bytes"`
	HeapSys            uint64     `json:"go_heap_sys_bytes"`
	TotalAlloc         uint64     `json:"go_total_alloc_bytes"`
	NumGC              uint32     `json:"go_num_gc"`
	MappingRSS         uint64     `json:"queue_mapping_rss_bytes"`
	MappingValid       bool       `json:"queue_mapping_valid"`
	MappingUnavailable string     `json:"mapping_unavailable,omitempty"`
	Main               fileSample `json:"main"`
	WAL                fileSample `json:"wal"`
	SHM                fileSample `json:"shm"`
}

func readResources(directory string) (resourceSample, error) {
	result := resourceSample{At: time.Now().UTC()}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	result.HeapAlloc = memory.HeapAlloc
	result.HeapInuse = memory.HeapInuse
	result.HeapSys = memory.HeapSys
	result.TotalAlloc = memory.TotalAlloc
	result.NumGC = memory.NumGC
	statm, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return result, err
	}
	fields := strings.Fields(string(statm))
	if len(fields) < 2 {
		return result, errors.New("statm lacks resident pages")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return result, err
	}
	result.RSS = pages * uint64(os.Getpagesize())
	result.MappingRSS, err = queueMappingRSS(directory)
	result.MappingValid = err == nil
	if err != nil {
		result.MappingUnavailable = err.Error()
	}
	result.Main, err = sampleFile(filepath.Join(directory, "queue.sqlite3"))
	if err != nil {
		return result, err
	}
	result.WAL, err = sampleFile(filepath.Join(directory, "queue.sqlite3-wal"))
	if err != nil {
		return result, err
	}
	result.SHM, err = sampleFile(filepath.Join(directory, "queue.sqlite3-shm"))
	return result, err
}

func sampleFile(path string) (fileSample, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return fileSample{}, nil
	}
	if err != nil {
		return fileSample{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileSample{}, errors.New("stat lacks allocated block count")
	}
	return fileSample{Exists: true, Length: info.Size(), Allocated: stat.Blocks * 512}, nil
}

func queueMappingRSS(directory string) (uint64, error) {
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return 0, fmt.Errorf("canonical mapping directory: %w", err)
	}
	directory, err = filepath.Abs(directory)
	if err != nil {
		return 0, err
	}
	paths := make(map[string]struct{}, 3)
	for _, name := range []string{"queue.sqlite3", "queue.sqlite3-wal", "queue.sqlite3-shm"} {
		path := filepath.Join(directory, name)
		canonical, err := filepath.EvalSymlinks(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("canonical mapping path: %w", err)
		}
		if err == nil {
			path = canonical
		}
		// proc escapes newlines ambiguously and appends this suffix to deleted files.
		if strings.ContainsAny(path, "\n\r") || strings.Contains(path, `\012`) ||
			strings.HasSuffix(path, " (deleted)") {
			return 0, fmt.Errorf("mapping path cannot be attributed safely: %q", path)
		}
		paths[path] = struct{}{}
	}
	file, err := os.Open("/proc/self/smaps")
	if err != nil {
		return 0, err
	}
	// This read-only proc descriptor has no buffered writes to lose.
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	var total uint64
	var queueMapping bool
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && strings.Contains(fields[0], "-") {
			path := smapsPath(scanner.Text())
			_, queueMapping = paths[path]
			_, knownPath := paths[strings.TrimSuffix(path, " (deleted)")]
			deletedQueueMapping := knownPath && strings.HasSuffix(path, " (deleted)")
			if deletedQueueMapping {
				return 0, fmt.Errorf("queue mapping was deleted: %q", path)
			}
		}
		if !queueMapping || len(fields) != 3 || fields[0] != "Rss:" {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("smaps RSS: %w", err)
		}
		total += value * 1024
	}
	return total, scanner.Err()
}

// smapsPath preserves whitespace within the pathname after the five fixed fields.
func smapsPath(header string) string {
	for range 5 {
		end := strings.IndexAny(header, " \t")
		if end < 0 {
			return ""
		}
		header = strings.TrimLeft(header[end:], " \t")
	}
	return header
}
