package backup

import (
	"fmt"
	"local/backup/lib/logging"
)

type backupOp int

const (
	backupOpNone backupOp = iota
	backupOpAdd
	backupOpChange
	backupOpRemove
)

type backupReason int

const (
	backupReasonNone backupReason = iota
	backupReasonNew
	backupReasonModtime
	backupReasonHash
)

type backupSummary struct {
	FilesAdded   []string
	FilesChanged []string
	FilesRemoved []string
}

func (s *backupSummary) AddFile(path string, op backupOp) {
	switch op {
	case backupOpAdd:
		s.FilesAdded = append(s.FilesAdded, path)
	case backupOpChange:
		s.FilesChanged = append(s.FilesChanged, path)
	case backupOpRemove:
		s.FilesRemoved = append(s.FilesRemoved, path)
	case backupOpNone:
		// Do nothing
	default:
		panic(fmt.Sprintf("unknown backup op: %d", op))
	}
}

func (s *backupSummary) Print(logger logging.Logger) {
	if len(s.FilesAdded) > 0 {
		logger.Infof("Files added:")
		for _, file := range s.FilesAdded {
			logger.Infof("  %s", file)
		}
	} else {
		logger.Infof("No files added")
	}
	if len(s.FilesChanged) > 0 {
		logger.Infof("Files changed:")
		for _, file := range s.FilesChanged {
			logger.Infof("  %s", file)
		}
	} else {
		logger.Infof("No files changed")
	}
	if len(s.FilesRemoved) > 0 {
		logger.Infof("Files removed:")
		for _, file := range s.FilesRemoved {
			logger.Infof("  %s", file)
		}
	} else {
		logger.Infof("No files removed")
	}
}
