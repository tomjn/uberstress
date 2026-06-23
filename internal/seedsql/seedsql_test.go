package seedsql

import (
	"strings"
	"testing"

	"github.com/tomjn/uberstress/internal/proto"
)

func TestGenerateContainsCountPrefixAndHash(t *testing.T) {
	sql := Generate(500, "uberstress_", "stresspw")
	if !strings.Contains(sql, "SET @n := 500;") {
		t.Errorf("expected count in SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "CONCAT('uberstress_', LPAD(i, 5, '0'))") {
		t.Errorf("expected prefix in CONCAT, got:\n%s", sql)
	}
	hash := proto.EncodePassword("stresspw")
	if !strings.Contains(sql, "'"+hash+"'") {
		t.Errorf("expected password hash %q in SQL, got:\n%s", hash, sql)
	}
	// Sanity: the well-known default-password hash.
	if hash != "uduVAmYKNKjEAUm/FJVcpA==" {
		t.Errorf("unexpected hash for stresspw: %s", hash)
	}
}

func TestGenerateHonoursCustomPrefixAndPassword(t *testing.T) {
	sql := Generate(1, "load_", "secret")
	if !strings.Contains(sql, "CONCAT('load_', LPAD(i, 5, '0'))") {
		t.Errorf("custom prefix missing:\n%s", sql)
	}
	if strings.Contains(sql, "uduVAmYKNKjEAUm/FJVcpA==") {
		t.Errorf("default hash should not appear for custom password:\n%s", sql)
	}
}
