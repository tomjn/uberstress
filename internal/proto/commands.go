package proto

import (
	"strings"
	"time"
)

// This file holds builders/round-trips for the session commands the workloads
// drive. Each round-trip method sends a command and blocks until its expected
// response token, returning the measured latency. Where a command's only
// observable result is a channel broadcast (SAY -> SAID), correlation is
// best-effort by matching the channel + our own payload.

// Ping sends PING and waits for PONG, returning the round-trip latency. PING is
// allowed pre-login and touches no database, so its latency is a direct probe
// of reactor responsiveness (head-of-line blocking).
func (c *Client) Ping(timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("PING"); err != nil {
		return 0, err
	}
	if _, _, err := c.Expect(timeout, func(l string) bool {
		return l == "PONG" || strings.HasPrefix(l, "PONG ")
	}); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// Join enters a channel and waits for the JOIN acknowledgement.
func (c *Client) Join(channel string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("JOIN %s", channel); err != nil {
		return 0, err
	}
	// Server echoes JOIN <channel> back to the joiner on success, or FAILED/
	// JOINFAILED on error.
	if _, _, err := c.Expect(timeout, func(l string) bool {
		return strings.HasPrefix(l, "JOIN "+channel) ||
			strings.HasPrefix(l, "JOINFAILED") ||
			strings.HasPrefix(l, "FAILED")
	}); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// Say posts a message to a channel and waits for the SAID echo carrying our
// payload back, approximating end-to-end say+broadcast latency.
func (c *Client) Say(channel, msg string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("SAY %s %s", channel, msg); err != nil {
		return 0, err
	}
	if _, _, err := c.Expect(timeout, func(l string) bool {
		return strings.HasPrefix(l, "SAID "+channel+" ") && strings.HasSuffix(l, msg)
	}); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}
