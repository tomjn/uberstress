package workload

import (
	"fmt"
	"strconv"
)

func itoa(i int) string { return strconv.Itoa(i) }

func btoa(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// accountName returns the deterministic account name for connection id.
func accountName(prefix string, id int) string {
	if prefix == "" {
		prefix = "uberstress_"
	}
	return fmt.Sprintf("%s%05d", prefix, id)
}

// channelName returns the channel a connection joins given a base name and the
// number of distinct channels to spread across.
func channelName(base string, id, channels int) string {
	if base == "" {
		base = "stress"
	}
	if channels <= 1 {
		return base
	}
	return fmt.Sprintf("%s%d", base, id%channels)
}
