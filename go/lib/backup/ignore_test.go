package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIgnoreFile(t *testing.T) {
	patterns := []string{
		"-ignore.txt$",
		"subdir-to-ignore/",
	}
	ignoreFile, err := LoadIgnoreFileFromString(strings.Join(patterns, "\n"))
	if err != nil {
		t.Fatalf("error loading ignore file: %v", err)
	}

	assert.False(t, ignoreFile.IsIgnored("a.txt"))
	assert.False(t, ignoreFile.IsIgnored("subdir/a.txt"))
	assert.False(t, ignoreFile.IsIgnored("a-ignore.md"))
	assert.True(t, ignoreFile.IsIgnored("subdir-to-ignore/a.txt"))
	assert.True(t, ignoreFile.IsIgnored("a-ignore.txt"))
	assert.True(t, ignoreFile.IsIgnored("subdir-to-ignore/a-ignore.txt"))

	// Also make sure that the ignore file itself is ignored.
	assert.True(t, ignoreFile.IsIgnored(".dbignore"))
}

func TestRoundTrip_IgnoreFile(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	// A few files in the root, one of which should be ignored
	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b-ignore.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	// A few files in a subdirectory, one of which should be ignored
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/c-ignore.txt"), 25))

	// A few files in another subdirectory, all of which should be ignored due to the subdirectory
	// being in the .dbignore file.
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/c-ignore.txt"), 25))

	// Create a .dbignore file in the root
	dbignorePath := filepath.Join(testBaseDir, ".dbignore")
	must(os.WriteFile(dbignorePath, []byte("-ignore.txt$\nsubdir-2/"), 0644))

	// Load the same ignore file that the backup will use so the round-trip comparison knows which
	// files are expected to be excluded from the recovery directory.
	ignoreFile, err := LoadIgnoreFile(dbignorePath)
	must(err)
	config.IgnoreFile = ignoreFile

	roundTripTest(config, t)

	// The backup should contain exactly the four files that were _not_ ignored (a.txt, c.txt,
	// subdir-1/a.txt, subdir-1/b.txt), each as a single-file batch. The ignored files (b-ignore.txt,
	// subdir-1/c-ignore.txt, and everything under subdir-2) must not produce any batches.
	assertBatchCount(t, config.DBFile, config.FullS3Prefix, 4)
}

// TestRoundTrip_NoIgnoreFile makes sure backups still work when there is no .dbignore file present
// (i.e. the ignore file is nil).
func TestRoundTrip_NoIgnoreFile(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/b.txt"), 9))

	roundTripTest(config, t)
}
