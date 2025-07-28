package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"local/backup/lib/util"
)

func TestRoundTrip_Basic_SingleFileBatches(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	roundTripTest(config, t)
}

func TestRoundTrip_Basic_MultiFileBatch(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	config.SizeThreshold = 100000
	roundTripTest(config, t)
}

func TestRoundTrip_WithSubdirectories_AllSingleFileBatches(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/c.txt"), 25))

	roundTripTest(config, t)
}

func TestRoundTrip_WithDeepSubdirectories_AllSingleFileBatches(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/with/many/directories/c.txt"), 25))

	roundTripTest(config, t)
}

func TestRoundTrip_SomeMultiFileBatches(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	// These should get grouped
	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	// These should _not_ get grouped because one of the files is too big
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt"), 2000))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	// These should get grouped together separately from the top-level files
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/seven/eight/nine/c.txt"), 25))

	config.SizeThreshold = 1000
	roundTripTest(config, t)
}

func TestRoundTrip_WithAddsAndDeletes(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	// These should get grouped
	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	// These should _not_ get grouped because one of the files is too big
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt"), 2000))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	// These should get grouped together separately from the top-level files
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/seven/eight/nine/c.txt"), 25))

	config.SizeThreshold = 1000
	roundTripTest(config, t)

	// Add and remove files such that the batching strategy does not change
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/top.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/four/five/six/d.txt"), 9))
	must(os.Remove(filepath.Join(testBaseDir, "subdir-2/four/five/six/b.txt")))
	// Make sure to also do this among the files that are single-file batches (the above are from
	// multi-file batches)
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/d.txt"), 10))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/ham/bur/ger/withcheese.txt"), 13))
	must(os.Remove(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt")))

	roundTripTest(config, t)
}

func TestRoundTrip_BatchingChangesAcrossRuns(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	// These should get grouped
	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	// These should _not_ get grouped because one of the files is too big
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt"), 2000))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	// These should get grouped together separately from the top-level files
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/seven/eight/nine/c.txt"), 25))

	config.SizeThreshold = 1000
	roundTripTest(config, t)

	// Make sure the batching strategy is as expected (described above)
	assertBatchCount(t, config.DBFile, config.FullS3Prefix, 6)

	// Remove the large file down the three, causing the entire directory hierarchy to collapse into
	// one batch. Also tests that file deletion is working properly.
	must(os.Remove(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt")))

	// Run the test again, _without_ clearing the bucket (so we effectively get the same behavior as a
	// non-fresh run in real life).
	config.LeaveBucketContents = true
	roundTripTest(config, t)

	// Now that we've removed the large file, we should have one big batch
	assertBatchCount(t, config.DBFile, config.FullS3Prefix, 1)
}

func TestRoundTrip_SizeThresholdChanges(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	must(createTestFile(filepath.Join(testBaseDir, "a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/four/five/six/big.txt"), 2000))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-1/seven/eight/nine/c.txt"), 25))

	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/one/two/three/a.txt"), 5))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/four/five/six/b.txt"), 9))
	must(createTestFile(filepath.Join(testBaseDir, "subdir-2/seven/eight/nine/c.txt"), 25))

	config.SizeThreshold = 100000
	roundTripTest(config, t)

	// There should only be one batch, since the threshold is high
	assertBatchCount(t, config.DBFile, config.FullS3Prefix, 1)

	// Run the test again, _without_ clearing the bucket (so we effectively get the same behavior as a
	// non-fresh run in real life).
	config.LeaveBucketContents = true
	config.SizeThreshold = 1000
	roundTripTest(config, t)

	// Now that we've reduced the size threshold, we should have two grouped batches and four files as
	// single-file batches
	assertBatchCount(t, config.DBFile, config.FullS3Prefix, 6)

	// Change it back and make sure things still work as expected
	config.LeaveBucketContents = true
	config.SizeThreshold = 100000
	roundTripTest(config, t)
	assertBatchCount(t, config.DBFile, config.FullS3Prefix, 1)
}

func TestRoundTrip_MultiRun(t *testing.T) {
	config := getDefaultTestConfig()
	defer config.Cleanup()
	testBaseDir := config.TestBaseDir

	type runFileSpec struct {
		Path string
		Size int
	}
	type runSpec struct {
		Files []runFileSpec
	}
	runSpecs := []runSpec{
		{
			Files: []runFileSpec{
				{Path: "a.txt", Size: 5},
				{Path: "b.txt", Size: 9},
				{Path: "c.txt", Size: 25},
			},
		},
	}
	runSpecs = append(runSpecs, runSpec{
		Files: append(
			[]runFileSpec{
				{Path: "subdir-1/one/two/three/a.txt", Size: 5},
				{Path: "subdir-1/four/five/six/b.txt", Size: 9},
				{Path: "subdir-1/seven/eight/nine/c.txt", Size: 25},
				{Path: "subdir-2/with/many/directories/a.txt", Size: 5},
				{Path: "subdir-2/with/many/directories/b.txt", Size: 9},
				{Path: "subdir-2/with/many/directories/c.txt", Size: 25},
			},
			runSpecs[0].Files...,
		),
	})
	runSpecs = append(runSpecs, runSpec{
		Files: append(
			[]runFileSpec{},
			// Remove all files that contain "b.txt" in the name, and rezise the a.txt files to be bigger.
			util.Map(
				util.Filter(runSpecs[0].Files, func(f runFileSpec) bool {
					return !strings.Contains(f.Path, "b.txt")
				}),
				func(f runFileSpec) runFileSpec {
					if strings.Contains(f.Path, "a.txt") {
						f.Size += 100
					}
					return f
				},
			)...,
		),
	})

	for i, runSpec := range runSpecs {
		for _, fileSpec := range runSpec.Files {
			must(createTestFile(filepath.Join(testBaseDir, fileSpec.Path), fileSpec.Size))
		}
		fmt.Printf("+++ running round trip test for run %d\n", i)
		roundTripTest(config, t)
		fmt.Printf("--- finished round trip test for run %d\n", i)
	}
}
