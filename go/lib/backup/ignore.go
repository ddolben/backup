package backup

import (
	"os"
	"regexp"
	"strings"
)

type IgnoreFile struct {
	Ignore []*regexp.Regexp
}

func (i *IgnoreFile) IsIgnored(path string) bool {
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
	ignore := strings.Split(str, "\n")
	// Ignore the ignore file itself.
	ignore = append(ignore, `\.dbignore$`)

	ignoreRegexes := make([]*regexp.Regexp, len(ignore))
	for i, pattern := range ignore {
		ignoreRegexes[i] = regexp.MustCompile(pattern)
	}

	return &IgnoreFile{Ignore: ignoreRegexes}, nil
}
