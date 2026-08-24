package gdbserial

import (
	"errors"
	"io"
	"testing"
	"time"
)

func ttdWaitInit(t *testing.T, initch <-chan ttdInit) ttdInit {
	t.Helper()
	select {
	case init := <-initch:
		return init
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for init")
		return ttdInit{}
	}
}

func TestTtdStdoutParserFindsListeningLine(t *testing.T) {
	pr, pw := io.Pipe()
	initch := make(chan ttdInit, 1)
	go ttdStdoutParser(pr, initch, true)

	go func() {
		pw.Write([]byte("Opening trace: x.run\n"))
		pw.Write([]byte("Listening on 127.0.0.1:54321\n"))
	}()

	init := ttdWaitInit(t, initch)
	if init.err != nil {
		t.Fatalf("unexpected error: %v", init.err)
	}
	if init.port != "127.0.0.1:54321" {
		t.Fatalf("port = %q, want %q", init.port, "127.0.0.1:54321")
	}
	// Unblock the post-handshake drain.
	pw.Close()
}

func TestTtdStdoutParserReportsEarlyExit(t *testing.T) {
	pr, pw := io.Pipe()
	initch := make(chan ttdInit, 1)
	go ttdStdoutParser(pr, initch, true)

	// Closing the pipe without a Listening line models ttd-gdbserver
	// exiting before it became ready.
	pw.Close()

	init := ttdWaitInit(t, initch)
	if init.err == nil {
		t.Fatal("expected an error when the server exits before listening")
	}
}

func TestTtdStdoutParserMalformedListeningLine(t *testing.T) {
	pr, pw := io.Pipe()
	initch := make(chan ttdInit, 1)
	go ttdStdoutParser(pr, initch, true)

	// A Listening line without an address must be reported as an error,
	// not as an empty port to Dial.
	pw.Write([]byte("Listening on \n"))
	pw.Close()

	init := ttdWaitInit(t, initch)
	var malformed *ErrMalformedTtdListeningLine
	if !errors.As(init.err, &malformed) {
		t.Fatalf("expected ErrMalformedTtdListeningLine, got %v", init.err)
	}
}
