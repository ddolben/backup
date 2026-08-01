package backup

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

type IgnoreFile struct {
	Ignore []*regexp.Regexp
}

func (i *IgnoreFile) IsIgnored(path string) bool {
	// A nil IgnoreFile means there was no .dbignore file, so nothing is ignored.
	if i == nil {
		return false
	}
	for _, regex := range i.Ignore {
		if regex.MatchString(path) {
			return true
		}
	}
	return false
}

func LoadIgnoreFile(path string) (*IgnoreFile, error) {
	ignoreFile, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return LoadIgnoreFileFromString(string(ignoreFile))
}

func LoadIgnoreFileFromString(str string) (*IgnoreFile, error) {
	lines := strings.Split(str, "\n")
	// Ignore the ignore file itself.
	lines = append(lines, `\.dbignore$`)

	var ignoreRegexes []*regexp.Regexp
	for _, pattern := range lines {
		// Skip empty (or whitespace-only) lines. An empty pattern compiles to a regex that matches
		// every path, which would silently exclude the entire backup. Trimming also strips stray
		// carriage returns and trailing whitespace from each pattern.
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid ignore pattern %q: %w", pattern, err)
		}
		ignoreRegexes = append(ignoreRegexes, regex)
	}

	return &IgnoreFile{Ignore: ignoreRegexes}, nil
}
