package fh

import (
	"fmt"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// ResolvePath {"" -> ".", "~..." -> "user.HomeDir..."} -> Abs
func ResolvePath(path string, usr *user.User) (string, error) {
	var err error
	if path == "" {
		path = "."
	}
	if strings.HasPrefix(path, "~") {
		if usr == nil {
			if userName := os.Getenv("SUDO_USER"); userName != "" { // os.UserHomeDir doesn't work with sudo ... os.Getuid() == 0
				usr, err = user.Lookup(userName)
			} else {
				usr, err = user.Current()
			}
			if err != nil {
				return path, fmt.Errorf("resolving path [%s] failed due to inability to get user info: %w", path, err)
			}
		}
		path = usr.HomeDir + path[1:]
	}
	return filepath.Abs(path)
}

// IsDirectory checks whether path is directory and exists
func IsDirectory(path string) (b bool, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, err
		}
	}
	if !fi.IsDir() {
		return false, fmt.Errorf(`not a directory: %v`, path)
	}
	return true, nil
}

// BytesToHuman converts 1024 to '1 KiB' etc
func BytesToHuman(src uint64) string {
	if src < 10 {
		return fmt.Sprintf("%d B", src)
	}

	s := float64(src)
	base := float64(1024)
	sizes := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

	e := math.Floor(math.Log(s) / math.Log(base))
	suffix := sizes[int(e)]
	val := math.Floor(s/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f %s"
	if val < 10 {
		f = "%.1f %s"
	}

	return fmt.Sprintf(f, val, suffix)
}

// yes, I like Python)
//def sizeof_fmt(num, suffix='B'):
//	for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
//		if abs(num) < 1024.0:
//			return "%3.1f%s%s" % (num, unit, suffix)
//		num /= 1024.0
//	return "%.1f%s%s" % (num, 'Yi', suffix)
