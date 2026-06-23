package proto

import (
	"fmt"
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

// Ignore adds target to our ignore list (an async INSERT on the server) and
// waits for the IGNORE echo. The server first broadcasts ADDUSER/REMOVEUSER for
// the now-hidden user; Expect skips those. A SERVERMSG (e.g. "User is already
// ignored.", "No such user.") is matched too -- and returned as an error -- so a
// rejection fails fast instead of blocking until timeout.
func (c *Client) Ignore(target string, timeout time.Duration) (time.Duration, error) {
	return c.socialTagWrite("IGNORE", "IGNORE userName="+target, target, timeout)
}

// Unignore removes target from our ignore list (an async DELETE) and waits for
// the UNIGNORE echo, with the same SERVERMSG fast-fail handling as Ignore.
func (c *Client) Unignore(target string, timeout time.Duration) (time.Duration, error) {
	return c.socialTagWrite("UNIGNORE", "UNIGNORE userName="+target, target, timeout)
}

// socialTagWrite sends a userName-tagged social mutation and waits for either
// its echo (success) or a SERVERMSG (rejection, returned as an error).
func (c *Client) socialTagWrite(cmd, echo, target string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("%s userName=%s", cmd, target); err != nil {
		return 0, err
	}
	line, _, err := c.Expect(timeout, func(l string) bool {
		return l == echo || strings.HasPrefix(l, "SERVERMSG ")
	})
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(line, "SERVERMSG ") {
		return 0, fmt.Errorf("%s rejected: %s", cmd, line)
	}
	return time.Since(start), nil
}

// IgnoreList requests the ignore list (an async read) and waits for the
// terminating IGNORELISTEND, timing the full read round-trip.
func (c *Client) IgnoreList(timeout time.Duration) (time.Duration, error) {
	return c.socialListRead("IGNORELIST", "IGNORELISTEND", timeout)
}

// FriendList requests the friend list (an async read) and waits for the
// terminating FRIENDLISTEND.
func (c *Client) FriendList(timeout time.Duration) (time.Duration, error) {
	return c.socialListRead("FRIENDLIST", "FRIENDLISTEND", timeout)
}

// socialListRead sends a parameterless list command and waits for its END
// terminator.
func (c *Client) socialListRead(cmd, end string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("%s", cmd); err != nil {
		return 0, err
	}
	if _, _, err := c.Expect(timeout, func(l string) bool { return l == end }); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// OpenBattle hosts a battle and returns its server-assigned battle id alongside
// the round-trip latency. OPENBATTLE requires a TLS connection (see
// Protocol.py:2153), so the caller must StartTLS first. The battle params
// (type/natType/key/port/maxplayers/modhash/rank/maphash + engine\tversion\tmap)
// are fixed to valid stress values; only the title varies. Success is the
// OPENBATTLE <id> echo; failure is OPENBATTLEFAILED or a SERVERMSG (e.g. the
// TLS-required message).
func (c *Client) OpenBattle(title string, timeout time.Duration) (string, time.Duration, error) {
	start := time.Now()
	// type natType key port maxplayers modhash rank maphash <engine\tversion\tmap\ttitle\tmod>
	if err := c.Send("OPENBATTLE 0 0 * 8452 8 1234567 0 0 spring\t105.0\tstressmap\t%s\tstressmod", title); err != nil {
		return "", 0, err
	}
	line, _, err := c.Expect(timeout, func(l string) bool {
		return strings.HasPrefix(l, "OPENBATTLE ") ||
			strings.HasPrefix(l, "OPENBATTLEFAILED") ||
			strings.HasPrefix(l, "SERVERMSG ")
	})
	if err != nil {
		return "", 0, err
	}
	if !strings.HasPrefix(line, "OPENBATTLE ") {
		return "", 0, fmt.Errorf("open battle failed: %s", line)
	}
	return strings.TrimPrefix(line, "OPENBATTLE "), time.Since(start), nil
}

// JoinBattle joins battleID and waits for the JOINBATTLE <id> <hash> ack. With
// our compat flags the host receives no JOINBATTLEREQUEST, so the join proceeds
// without host approval (Protocol.py:2231). Failure is JOINBATTLEFAILED.
func (c *Client) JoinBattle(battleID string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("JOINBATTLE %s", battleID); err != nil {
		return 0, err
	}
	line, _, err := c.Expect(timeout, func(l string) bool {
		return strings.HasPrefix(l, "JOINBATTLE "+battleID+" ") ||
			strings.HasPrefix(l, "JOINBATTLEFAILED")
	})
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(line, "JOINBATTLEFAILED") {
		return 0, fmt.Errorf("join battle failed: %s", line)
	}
	return time.Since(start), nil
}

// MyBattleStatus sets the caller's in-battle status and waits for the
// CLIENTBATTLESTATUS <username> echo (the server always echoes, broadcasting to
// the battle or replying directly when nothing changed). username is needed to
// match our own echo amid other members' status broadcasts.
func (c *Client) MyBattleStatus(username, status, teamColor string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("MYBATTLESTATUS %s %s", status, teamColor); err != nil {
		return 0, err
	}
	line, _, err := c.Expect(timeout, func(l string) bool {
		return strings.HasPrefix(l, "CLIENTBATTLESTATUS "+username+" ") ||
			strings.HasPrefix(l, "FAILED ")
	})
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(line, "FAILED ") {
		return 0, fmt.Errorf("mybattlestatus failed: %s", line)
	}
	return time.Since(start), nil
}

// LeaveBattle leaves the current battle and waits for the LEFTBATTLE <id>
// <username> broadcast, which the leaver also receives.
func (c *Client) LeaveBattle(battleID, username string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.Send("LEAVEBATTLE"); err != nil {
		return 0, err
	}
	line, _, err := c.Expect(timeout, func(l string) bool {
		return l == "LEFTBATTLE "+battleID+" "+username ||
			strings.HasPrefix(l, "FAILED ")
	})
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(line, "FAILED ") {
		return 0, fmt.Errorf("leave battle failed: %s", line)
	}
	return time.Since(start), nil
}
