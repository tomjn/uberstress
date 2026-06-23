package proto

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"strings"
	"time"
)

// StartTLS upgrades the connection to TLS in place. The lobby's STARTTLS has no
// plaintext acknowledgement: the server begins the TLS handshake the instant it
// reads the command, then sends a fresh TASSERVER greeting over the encrypted
// channel (Protocol.py in_STARTTLS). So we send STARTTLS, immediately wrap the
// socket as a TLS client, complete the handshake, swap in the encrypted
// conn+reader, and consume the post-upgrade greeting.
//
// This must be called on a quiescent connection -- in practice right after Dial,
// before LOGIN -- so no plaintext server bytes are buffered when we swap to TLS.
// We guard that invariant: any buffered bytes would be stranded in the old
// reader (and, worse, the kernel may hold server bytes the TLS handshake would
// misread), so we refuse rather than corrupt the stream.
//
// The server certificate is self-signed, so verification is skipped -- this is a
// load generator, not a security boundary.
func (c *Client) StartTLS(timeout time.Duration) error {
	if n := c.reader.Buffered(); n != 0 {
		return fmt.Errorf("proto: %d bytes buffered before STARTTLS; not quiescent", n)
	}
	if err := c.Send("STARTTLS"); err != nil {
		return err
	}

	tconn := tls.Client(c.conn, &tls.Config{InsecureSkipVerify: true})
	if err := tconn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if err := tconn.Handshake(); err != nil {
		return fmt.Errorf("proto: TLS handshake: %w", err)
	}
	if err := tconn.SetDeadline(time.Time{}); err != nil {
		return err
	}

	c.conn = tconn
	c.reader = bufio.NewReader(tconn)

	if _, _, err := c.Expect(timeout, func(l string) bool {
		return strings.HasPrefix(l, "TASSERVER")
	}); err != nil {
		return fmt.Errorf("proto: waiting for post-TLS greeting: %w", err)
	}
	return nil
}
