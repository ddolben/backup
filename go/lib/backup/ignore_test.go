package backup

import (
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

/*
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
	must(os.WriteFile(filepath.Join(testBaseDir, ".dbignore"), []byte("-ignore.txt$\nsubdir-2/"), 0644))

	roundTripTest(config, t)
}
*/
