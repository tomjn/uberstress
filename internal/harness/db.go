// Package harness orchestrates a single local benchmark run end-to-end: reset
// the database, launch a uberserver version, drive load against it, tear it
// down, and emit a tagged report. Every address and credential is configurable
// so the same flow can later target a dedicated remote environment instead of
// localhost.
package harness

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
)

// DBConfig describes how to reach MariaDB both as a SQLAlchemy URL (for the
// server) and as `mysql` CLI arguments (for the drop/create reset).
//
// Defaults target a local Homebrew MariaDB using root/root over TCP, which is
// what uberserver's own local setup uses. The Driver defaults to
// "mysql+pymysql" because the dev venv ships PyMySQL, not mysqlclient; a
// production environment with mysqlclient can override it to "mysql".
type DBConfig struct {
	Driver   string // SQLAlchemy driver, e.g. "mysql+pymysql" or "mysql"
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	MySQLBin string // path to the mysql CLI (default "mysql")
}

// DefaultDBConfig returns the local Homebrew MariaDB defaults.
func DefaultDBConfig() DBConfig {
	return DBConfig{
		Driver:   "mysql+pymysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "root",
		Name:     "uberstress_ab",
		MySQLBin: "mysql",
	}
}

// SQLURL renders the SQLAlchemy connection URL passed to the server via -s.
func (d DBConfig) SQLURL() string {
	auth := d.User
	if d.Password != "" {
		auth += ":" + d.Password
	}
	return fmt.Sprintf("%s://%s@%s:%d/%s?charset=utf8", d.Driver, auth, d.Host, d.Port, d.Name)
}

// Reset drops and recreates the target database so each run starts from a clean,
// identical schema. The server recreates its own tables on startup.
func (d DBConfig) Reset(ctx context.Context) error {
	stmt := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`; CREATE DATABASE `%s` CHARACTER SET utf8;", d.Name, d.Name)
	args := append(d.cliArgs(), "-e", stmt)
	out, err := exec.CommandContext(ctx, d.mysqlBin(), args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("db reset (%s): %v: %s", d.Name, err, out)
	}
	return nil
}

// cliArgs returns the connection arguments shared by mysql CLI invocations.
func (d DBConfig) cliArgs() []string {
	args := []string{"-h", d.Host, "-P", strconv.Itoa(d.Port), "-u", d.User}
	if d.Password != "" {
		args = append(args, "-p"+d.Password)
	}
	return args
}

func (d DBConfig) mysqlBin() string {
	if d.MySQLBin != "" {
		return d.MySQLBin
	}
	return "mysql"
}
