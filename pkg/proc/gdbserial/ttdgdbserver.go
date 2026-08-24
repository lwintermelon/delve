package gdbserial

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/go-delve/delve/pkg/config"
	"github.com/go-delve/delve/pkg/logflags"
	"github.com/go-delve/delve/pkg/proc"
)

const (
	// delveTtdServerEnvVar overrides the ttd-gdbserver executable.
	delveTtdServerEnvVar = "DELVE_TTD_SERVER"
	// delveTtdServerFlagsEnvVar can be used to pass additional command line
	// flags to ttd-gdbserver (split with shell-like quoting rules).
	delveTtdServerFlagsEnvVar = "DELVE_TTD_SERVER_FLAGS"
	// ttdListeningPrefix is the line ttd-gdbserver prints once it is ready.
	ttdListeningPrefix = "Listening on "
)

// ErrTtdServerNotFound is returned when the ttd-gdbserver executable cannot
// be located.
type ErrTtdServerNotFound struct {
	Err error
}

func (err *ErrTtdServerNotFound) Error() string {
	if err.Err != nil {
		return "ttd-gdbserver executable not found: " + err.Err.Error()
	}
	return "ttd-gdbserver executable not found: set DELVE_TTD_SERVER or add it to PATH"
}

func (err *ErrTtdServerNotFound) Unwrap() error {
	return err.Err
}

// ErrMalformedTtdListeningLine is returned when ttd-gdbserver prints its
// "Listening on" line without an address after the prefix.
type ErrMalformedTtdListeningLine struct {
	Line string
}

func (err *ErrMalformedTtdListeningLine) Error() string {
	return fmt.Sprintf("malformed listening line %q", err.Line)
}

// ttdServerPath returns the ttd-gdbserver executable to use.
func ttdServerPath() (string, error) {
	if p := os.Getenv(delveTtdServerEnvVar); p != "" {
		return p, nil
	}
	return exec.LookPath("ttd-gdbserver")
}

// TtdGdbserverReplay starts an instance of ttd-gdbserver on the given .run trace and
// connects to it. It is the TTD analogue of Replay (rr).
//
//	tracePath     path to the .run trace file
//	quiet         suppress ttd-gdbserver's stderr
//	debugInfoDirs extra directories to search for debug info
//	cmdline       the command line recorded in the session (informational)
//
// The recorded executable may be specified explicitly with exe; when empty it
// is requested from the stub via qXfer:exec-file (ttd-gdbserver reports the
// first module of the trace).
func TtdGdbserverReplay(tracePath string, quiet bool, debugInfoDirs []string, cmdline string, exe string) (*proc.TargetGroup, error) {
	server, err := ttdServerPath()
	if err != nil {
		return nil, &ErrTtdServerNotFound{Err: err}
	}

	args := []string{tracePath, "--listen", "127.0.0.1:0"}
	args = append(args, config.SplitQuotedFields(os.Getenv(delveTtdServerFlagsEnvVar), '"')...)

	logflags.GdbWireLogger().Debugf("executing %s %v", server, args)
	cmd := exec.Command(server, args...)
	cmd.SysProcAttr = sysProcAttr(false)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	initch := make(chan ttdInit, 1)
	go ttdStdoutParser(stdout, initch, quiet)
	go ttdStderrDrain(stderr, quiet)

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	init := <-initch
	if init.err != nil {
		_ = cmd.Process.Kill()
		return nil, init.err
	}

	p := newProcess(cmd.Process, false)
	// Marking tracedir enables Recorded(), reverse execution, checkpoints
	// and restart — the same flag Replay sets for rr.
	p.tracedir = tracePath
	// ttd-gdbserver speaks the post-5.8.0 qRRCmd style.
	p.conn.newRRCmdStyle = true

	tgt, err := p.Dial(init.port, exe, cmdline, 0, debugInfoDirs, proc.StopLaunched)
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
	return tgt, nil
}

type ttdInit struct {
	port string
	err  error
}

// ttdStdoutParser watches ttd-gdbserver's stdout for the
// "Listening on <addr>" line and reports the address. Lines printed before
// it are forwarded to stderr (unless quiet); after it is found the rest of
// stdout keeps being forwarded, so later output from the server is not lost.
func ttdStdoutParser(stdout io.ReadCloser, initch chan<- ttdInit, quiet bool) {
	rd := bufio.NewReader(stdout)

	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			initch <- ttdInit{err: errors.New("ttd-gdbserver exited before listening")}
			close(initch)
			_ = stdout.Close()
			return
		}

		if rest, ok := strings.CutPrefix(line, ttdListeningPrefix); ok {
			port := strings.TrimSpace(rest)
			if port == "" {
				initch <- ttdInit{err: &ErrMalformedTtdListeningLine{Line: strings.TrimRight(line, "\r\n")}}
				close(initch)
				_ = stdout.Close()
				return
			}
			initch <- ttdInit{port: port}
			close(initch)
			break
		}

		if !quiet {
			os.Stderr.WriteString(line)
		}
	}

	if quiet {
		_, _ = io.Copy(io.Discard, rd)
	} else {
		_, _ = io.Copy(os.Stdout, rd)
	}
	_ = stdout.Close()
}

// ttdStderrDrain forwards ttd-gdbserver's stderr to our stderr unless quiet.
func ttdStderrDrain(stderr io.ReadCloser, quiet bool) {
	defer stderr.Close()
	if quiet {
		_, _ = io.Copy(io.Discard, stderr)
		return
	}
	_, _ = io.Copy(os.Stderr, stderr)
}
