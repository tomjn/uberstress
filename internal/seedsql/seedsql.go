// Package seedsql generates the SQL that pre-seeds login accounts so uberstress
// can run with --register=false. It is the single source of truth for the
// account template and password encoding, shared by the `gen-seed-sql`
// subcommand (and re-used by the Coilbox GUI plugin via that subcommand).
package seedsql

import (
	"fmt"

	"github.com/tomjn/uberstress/internal/proto"
)

// Generate returns a runnable MySQL script that INSERT IGNOREs `count` accounts
// named "<prefix>NNNNN" (zero-padded), all sharing `password`. Accounts are
// created with access='user', i.e. already past ToS + email verification.
// Safe to re-run with a larger count: INSERT IGNORE skips existing usernames.
func Generate(count int, prefix, password string) string {
	hash := proto.EncodePassword(password)
	return fmt.Sprintf(`-- uberstress seed accounts (generated)
-- Pre-seed login accounts so uberstress can run with --register=false.
-- Run once per server instance (each A/B instance has its own DB):
--   mysql -u <user> -p <dbname> < this.sql
-- INSERT IGNORE skips usernames that already exist, so the pool only grows.

SET @n := %d;                                    -- account count (>= max --conns)
SET SESSION cte_max_recursion_depth = @n + 1;    -- lift the default 1000 CTE cap

INSERT IGNORE INTO users
  (username, `+"`password`"+`, register_date, last_login, last_ip,
   last_agent, last_sys_id, last_mac_id, ingame_time, access, email, bot)
SELECT
  CONCAT('%s', LPAD(i, 5, '0')),
  '%s',                                          -- base64(md5(password))
  NOW(), NOW(), '127.0.0.1',
  '', '', '', 0, 'user', NULL, 0
FROM (
  WITH RECURSIVE seq(i) AS (
    SELECT 0
    UNION ALL
    SELECT i + 1 FROM seq WHERE i + 1 < @n
  )
  SELECT i FROM seq
) AS s;
`, count, prefix, hash)
}
