package proto

import (
	"crypto/md5"
	"encoding/base64"
)

// EncodePassword returns base64(md5(password)), the legacy lobby password
// encoding. Replicated from the reference client tests/stresstest.py:40.
func EncodePassword(password string) string {
	sum := md5.Sum([]byte(password))
	return base64.StdEncoding.EncodeToString(sum[:])
}
