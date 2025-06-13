package utils

import (
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

// Of returns the size of 'v' in bytes.
// If there is an error during calculation, Of returns -1.
func SizeOf(v any) int {
	// Cache with every visited pointer so we don't count two pointers
	// to the same memory twice.
	cache := make(map[uintptr]bool)
	return sizeOf(reflect.Indirect(reflect.ValueOf(v)), cache)
}

// sizeOf returns the number of bytes the actual data represented by v occupies in memory.
// If there is an error, sizeOf returns -1.
func sizeOf(v reflect.Value, cache map[uintptr]bool) int {
	switch v.Kind() {
	case reflect.Array:
		sum := 0
		for i := 0; i < v.Len(); i++ {
			s := sizeOf(v.Index(i), cache)
			if s < 0 {
				return -1
			}
			sum += s
		}

		return sum + (v.Cap()-v.Len())*int(v.Type().Elem().Size())

	case reflect.Slice:
		// return 0 if this node has been visited already
		if cache[v.Pointer()] {
			return 0
		}
		cache[v.Pointer()] = true

		sum := 0
		for i := 0; i < v.Len(); i++ {
			s := sizeOf(v.Index(i), cache)
			if s < 0 {
				return -1
			}
			sum += s
		}

		sum += (v.Cap() - v.Len()) * int(v.Type().Elem().Size())

		return sum + int(v.Type().Size())

	case reflect.Struct:
		sum := 0
		for i, n := 0, v.NumField(); i < n; i++ {
			s := sizeOf(v.Field(i), cache)
			if s < 0 {
				return -1
			}
			sum += s
		}

		// Look for struct padding.
		padding := int(v.Type().Size())
		for i, n := 0, v.NumField(); i < n; i++ {
			padding -= int(v.Field(i).Type().Size())
		}

		return sum + padding

	case reflect.String:
		s := v.String()
		dataPtr := uintptr(unsafe.Pointer(&s))
		if cache[dataPtr] {
			return int(v.Type().Size())
		}
		cache[dataPtr] = true
		return len(s) + int(v.Type().Size())

	case reflect.Ptr:
		// return Ptr size if this node has been visited already (infinite recursion)
		if cache[v.Pointer()] {
			return int(v.Type().Size())
		}
		cache[v.Pointer()] = true
		if v.IsNil() {
			return int(reflect.New(v.Type()).Type().Size())
		}
		s := sizeOf(reflect.Indirect(v), cache)
		if s < 0 {
			return -1
		}
		return s + int(v.Type().Size())

	case reflect.Bool,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Int, reflect.Uint,
		reflect.Chan,
		reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.Func:
		return int(v.Type().Size())

	case reflect.Map:
		// return 0 if this node has been visited already (infinite recursion)
		if cache[v.Pointer()] {
			return 0
		}
		cache[v.Pointer()] = true
		sum := 0
		keys := v.MapKeys()
		for i := range keys {
			val := v.MapIndex(keys[i])
			// calculate size of key and value separately
			sv := sizeOf(val, cache)
			if sv < 0 {
				return -1
			}
			sum += sv
			sk := sizeOf(keys[i], cache)
			if sk < 0 {
				return -1
			}
			sum += sk
		}
		// Include overhead due to unused map buckets.  10.79 comes
		// from https://golang.org/src/runtime/map.go.
		return sum + int(v.Type().Size()) + int(float64(len(keys))*10.79)

	case reflect.Interface:
		return sizeOf(v.Elem(), cache) + int(v.Type().Size())
	}
	return -1
}

// DetermineSystemMemoryGB returns the total system memory in GB.
// Returns -1 if unable to determine memory.
func DetermineSystemMemoryGB() int64 {
	var memCmd *exec.Cmd
	var memOutput []byte
	var err error

	if runtime.GOOS == "linux" {
		// On Linux, use /proc/meminfo
		memCmd = exec.Command("grep", "MemTotal", "/proc/meminfo")
		memOutput, err = memCmd.Output()
		if err == nil {
			// Parse output like "MemTotal:       16384000 kB"
			parts := strings.Fields(string(memOutput))
			if len(parts) >= 2 {
				totalKb, err := strconv.ParseInt(parts[1], 10, 64)
				if err == nil {
					return totalKb / 1024 / 1024 // Convert KB to GB
				}
			}
		}
	} else if runtime.GOOS == "darwin" {
		// On macOS, use sysctl
		memCmd = exec.Command("sysctl", "-n", "hw.memsize")
		memOutput, err = memCmd.Output()
		if err == nil {
			totalBytes, err := strconv.ParseInt(strings.TrimSpace(string(memOutput)), 10, 64)
			if err == nil {
				return totalBytes / 1024 / 1024 / 1024 // Convert bytes to GB
			}
		}
	} else if runtime.GOOS == "windows" {
		// On Windows, use wmic
		memCmd = exec.Command("wmic", "OS", "get", "TotalVisibleMemorySize", "/Value")
		memOutput, err = memCmd.Output()
		if err == nil {
			// Parse output like "TotalVisibleMemorySize=16384000"
			outputStr := string(memOutput)
			parts := strings.Split(outputStr, "=")
			if len(parts) >= 2 {
				totalKb, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
				if err == nil {
					return totalKb / 1024 / 1024 // Convert KB to GB
				}
			}
		}
	}

	return -1 // Unable to determine
}
