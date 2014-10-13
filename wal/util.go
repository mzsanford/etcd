package wal

import (
	"fmt"
	"log"
	"os"
)

func Exist(dirpath string) bool {
	names, err := readDir(dirpath)
	if err != nil {
		return false
	}
	return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWalName(name)
		if err != nil {
			panic("parse correct name error")
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
func isValidSeq(names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWalName(name)
		if err != nil {
			panic("parse correct name error")
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

// readDir returns the filenames in wal directory.
func readDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	return names, nil
}

func checkWalNames(names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWalName(name); err != nil {
			log.Printf("parse %s: %v", name, err)
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

func parseWalName(str string) (seq, index uint64, err error) {
	var num int
	num, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	if num != 2 && err == nil {
		err = fmt.Errorf("bad wal name: %s", str)
	}
	return
}

func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
