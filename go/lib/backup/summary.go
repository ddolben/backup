package backup

import (
	"fmt"
	"local/backup/lib/logging"
)

type backupReason int

const (
	backupReasonNone backupReason = iota
	backupReasonAdded
	backupReasonChanged
	backupReasonRemoved
)

type backupSummary struct {
	FilesAdded   []string
	FilesChanged []string
	FilesRemoved []string
}

func (s *backupSummary) AddFile(path string, reason backupReason) {
	switch reason {
	case backupReasonAdded:
		s.FilesAdded = append(s.FilesAdded, path)
	case backupReasonChanged:
		s.FilesChanged = append(s.FilesChanged, path)
	case backupReasonRemoved:
		s.FilesRemoved = append(s.FilesRemoved, path)
	case backupReasonNone:
		// Do nothing
	default:
		panic(fmt.Sprintf("unknown backup reason: %d", reason))
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
