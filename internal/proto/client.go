// Package proto implements a minimal uberserver lobby protocol client suitable
// for load generation.
//
// Design: each Client is driven by exactly one goroutine. There is no
// background read loop; the owning goroutine performs blocking reads with
// deadlines via Expect. This keeps request/response serialization simple and
// makes a mid-stream STARTTLS upgrade trivial (just swap conn+reader between
// reads). Latency is measured by timing send -> expected-response-token, which
// is robust across both the old (synchronous) and new (async/deferred) server
// code paths -- unlike the protocol's #id echo, which is only attached when the
// reply is produced on the handler thread (see Protocol.py:382-385).
package proto

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// ErrTimeout is returned by Expect when no matching line arrives in time.
var ErrTimeout = errors.New("proto: timed out waiting for response")

// Client is a single lobby connection. Not safe for concurrent use; one
// goroutine owns it for its lifetime.
type Client struct {
	addr         string
	conn         net.Conn
	reader       *bufio.Reader
	writeTimeout time.Duration

	// ServerInfo holds the TASSERVER greeting line the server sends on connect.
	ServerInfo string
}

// Dial connects to addr and consumes the TASSERVER greeting.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr:         addr,
		conn:         conn,
		reader:       bufio.NewReader(conn),
		writeTimeout: 10 * time.Second,
	}
	line, _, err := c.Expect(timeout, func(l string) bool {
		return strings.HasPrefix(l, "TASSERVER")
	})
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("waiting for TASSERVER greeting: %w", err)
	}
	c.ServerInfo = line
	return c, nil
}

// Send writes a single command line (a trailing newline is appended).
func (c *Client) Send(format string, a ...any) error {
	s := fmt.Sprintf(format, a...)
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	_, err := c.conn.Write([]byte(s + "\n"))
	return err
}

// readLine reads one CRLF/LF-terminated line, bounded by timeout.
//
// Note: a deadline that fires mid-line yields a partial read that bufio cannot
// cleanly resume, so any timeout here should be treated as terminal for the
// connection by the caller (close and, if needed, redial). In practice lines
// are tiny and arrive whole; a timeout means the server is genuinely stalled --
// which is exactly the condition we are measuring.
func (c *Client) readLine(timeout time.Duration) (string, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", err
	}
	line, err := c.reader.ReadString('\n')
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return "", ErrTimeout
		}
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Expect reads lines until match returns true or timeout elapses. Non-matching
// lines (server broadcasts, state-dump entries) are returned in skipped so a
// caller can inspect them if needed.
func (c *Client) Expect(timeout time.Duration, match func(line string) bool) (matched string, skipped []string, err error) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", skipped, ErrTimeout
		}
		line, rerr := c.readLine(remaining)
		if rerr != nil {
			return "", skipped, rerr
		}
		if match(line) {
			return line, skipped, nil
		}
		skipped = append(skipped, line)
	}
}

// Exit politely closes the session.
func (c *Client) Exit() {
	_ = c.Send("EXIT")
	c.Close()
}

// Close tears down the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
