package proto

import (
	"fmt"
	"strings"
	"time"
)

// loginSentence is the tab-delimited trailer after the "0 *" args. The lobby
// protocol reads this as <lobbyID>\t<lastSeenID>\t<compatFlags>; we mirror the
// reference client (tests/stresstest.py:173) with compat flags "sp cl p".
const loginSentence = "uberstress\t0\tsp cl p"

// agreementReadDelay covers the server's mandatory "take at least a few seconds
// to read our terms of service" gate (Protocol.py:1253-1256, 2s since
// register_date). EnsureAccount waits this out; the hot-path Login never does.
const agreementReadDelay = 2100 * time.Millisecond

// Login authenticates an already-seeded account and waits for the login state
// dump to terminate with LOGININFOEND. Returns time-to-login (send LOGIN ->
// receive LOGININFOEND).
//
// Login is strict: if the server asks for agreement confirmation the account
// was not seeded, and that is returned as an error rather than paying the 2s
// ToS gate in the measured path.
func (c *Client) Login(username, password string, timeout time.Duration) (time.Duration, error) {
	enc := EncodePassword(password)
	start := time.Now()
	if err := c.sendLogin(username, enc); err != nil {
		return 0, err
	}
	line, _, err := c.Expect(timeout, isLoginVerdict)
	if err != nil {
		return 0, err
	}
	switch {
	case strings.HasPrefix(line, "DENIED"):
		return 0, fmt.Errorf("login denied: %s", line)
	case strings.HasPrefix(line, "AGREEMENTEND"):
		return 0, fmt.Errorf("login requires agreement; account not seeded")
	}
	if _, _, err := c.Expect(timeout, func(l string) bool { return l == "LOGININFOEND" }); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// Register performs a single REGISTER round-trip, returning its latency. A
// REGISTRATIONDENIED (e.g. name already taken) is returned as an error. Used by
// the register-storm scenario to measure async-register latency in isolation.
func (c *Client) Register(username, password string, timeout time.Duration) (time.Duration, error) {
	enc := EncodePassword(password)
	start := time.Now()
	if err := c.Send("REGISTER %s %s", username, enc); err != nil {
		return 0, err
	}
	line, _, err := c.Expect(timeout, isRegisterVerdict)
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(line, "REGISTRATIONDENIED") {
		return 0, fmt.Errorf("registration denied: %s", line)
	}
	return time.Since(start), nil
}

// EnsureAccount makes sure username exists and has confirmed the agreement, so
// later Login calls go straight to ACCEPTED. It is idempotent and absorbs the
// ~2s ToS gate, so it belongs in a setup phase, not the measured hot path.
func (c *Client) EnsureAccount(username, password string, timeout time.Duration) error {
	enc := EncodePassword(password)
	if err := c.Send("REGISTER %s %s", username, enc); err != nil {
		return err
	}
	// A REGISTRATIONDENIED here just means the account already exists (possibly
	// left in the unconfirmed 'agreement' state by a prior run), so we fall
	// through and drive a login either way to confirm the agreement if needed.
	if _, _, err := c.Expect(timeout, isRegisterVerdict); err != nil {
		return err
	}

	// Drive a login: a confirmed account returns ACCEPTED, a fresh/unconfirmed
	// one returns the agreement handshake.
	if err := c.sendLogin(username, enc); err != nil {
		return err
	}
	line, _, err := c.Expect(timeout, isLoginVerdict)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "AGREEMENTEND") {
		if strings.HasPrefix(line, "DENIED") {
			// "Already logged in." from a concurrent seed implies the account
			// exists and is confirmed; any other denial is a real failure.
			if strings.Contains(line, "Already logged in") {
				return nil
			}
			return fmt.Errorf("seed login denied: %s", line)
		}
		return nil // ACCEPTED: account already confirmed
	}

	time.Sleep(agreementReadDelay)
	if err := c.Send("CONFIRMAGREEMENT"); err != nil {
		return err
	}
	// Success -> login dump (LOGININFOEND); failure -> DENIED.
	res, _, err := c.Expect(timeout+agreementReadDelay, func(l string) bool {
		return l == "LOGININFOEND" || strings.HasPrefix(l, "DENIED")
	})
	if err != nil {
		return err
	}
	if strings.HasPrefix(res, "DENIED") {
		return fmt.Errorf("confirmagreement denied: %s", res)
	}
	return nil
}

func (c *Client) sendLogin(username, encodedPassword string) error {
	return c.Send("LOGIN %s %s 0 *\t%s", username, encodedPassword, loginSentence)
}

func isLoginVerdict(l string) bool {
	return strings.HasPrefix(l, "ACCEPTED") ||
		strings.HasPrefix(l, "DENIED") ||
		strings.HasPrefix(l, "AGREEMENTEND")
}

func isRegisterVerdict(l string) bool {
	return strings.HasPrefix(l, "REGISTRATIONACCEPTED") ||
		strings.HasPrefix(l, "REGISTRATIONDENIED")
}
