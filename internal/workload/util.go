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
