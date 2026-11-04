package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type sampleStats struct {
	Count    uint64 `json:"count"`
	MainMax  int64  `json:"main_max_bytes"`
	WALMax   int64  `json:"wal_max_bytes"`
	RSSMax   uint64 `json:"rss_max_bytes"`
	LastMain int64  `json:"last_main_bytes"`
	LastWAL  int64  `json:"last_wal_bytes"`
	LastRSS  uint64 `json:"last_rss_bytes"`
}

func (s *sampleStats) observe(directory string) error {
	main, err := fileLength(filepath.Join(directory, "queue.sqlite3"))
	if err != nil {
		return err
	}
	wal, err := fileLength(filepath.Join(directory, "queue.sqlite3-wal"))
	if err != nil {
		return err
	}
	contents, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return err
	}
	fields := strings.Fields(string(contents))
	if len(fields) < 2 {
		return errors.New("invalid /proc/self/statm")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return fmt.Errorf("RSS pages: %w", err)
	}
	rss := pages * uint64(os.Getpagesize())
	s.Count++
	s.MainMax = max(s.MainMax, main)
	s.WALMax = max(s.WALMax, wal)
	s.RSSMax = max(s.RSSMax, rss)
	s.LastMain, s.LastWAL, s.LastRSS = main, wal, rss
	return nil
}

func (s *sampleStats) merge(other sampleStats) {
	s.Count += other.Count
	s.MainMax = max(s.MainMax, other.MainMax)
	s.WALMax = max(s.WALMax, other.WALMax)
	s.RSSMax = max(s.RSSMax, other.RSSMax)
}

func fileLength(path string) (int64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func processCPU() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano()), nil
}

type samplerResult struct {
	stats sampleStats
	err   error
}
type resourceSampler struct {
	stop chan struct{}
	done chan samplerResult
}

func startSampler(directory string) resourceSampler {
	sampler := resourceSampler{stop: make(chan struct{}), done: make(chan samplerResult, 1)}
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		var samples sampleStats
		for {
			select {
			case <-sampler.stop:
				sampler.done <- samplerResult{stats: samples}
				return
			case <-ticker.C:
				if err := samples.observe(directory); err != nil {
					sampler.done <- samplerResult{stats: samples, err: err}
					return
				}
			}
		}
	}()
	return sampler
}

func (s resourceSampler) join() (sampleStats, error) {
	close(s.stop)
	result := <-s.done
	return result.stats, result.err
}
