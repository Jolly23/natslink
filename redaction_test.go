package natslink

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestMalformedProbeAndStartDoNotLeakCredentials(t *testing.T) {
	for _, raw := range []string{
		"nats://synthetic-secret@127.0.0.1:bad",
		"nats://synthetic-user:synthetic-password@127.0.0.1:bad",
		"nats://synthetic%2Dsecret@127.0.0.1:bad",
		"nats://synthetic%ZZsecret@127.0.0.1:4222",
	} {
		if err := Probe(raw); err == nil {
			t.Fatal("invalid Probe must fail")
		} else {
			assertNoCredential(t, err)
		}
		p, err := NewPublisher(PublisherOptions{Options: Options{URL: raw, Topic: "offline", Logger: benchLogger{}, ReportInterval: -1}})
		if err != nil {
			t.Fatal(err)
		}
		err = p.Start()
		p.Stop()
		if err == nil {
			t.Fatal("invalid Start must fail")
		}
		assertNoCredential(t, err)
	}
}

func TestRedactionCoversNestedAndDecodedErrors(t *testing.T) {
	cause := errors.New("dial rejected synthetic-secret at nats://synthetic%2Dsecret@127.0.0.1:bad")
	err := redactError("nats://synthetic%2Dsecret@127.0.0.1:bad", fmt.Errorf("nested: %w", cause))
	assertNoCredential(t, err)
	if !errors.Is(err, cause) {
		t.Fatal("redaction lost error classification")
	}
	if errors.Unwrap(err) != nil {
		t.Fatal("unsafe original error remains printable through Unwrap")
	}
	err = redactError("synthetic-secret@://invalid", errors.New("synthetic-secret@://invalid"))
	assertNoCredential(t, err)
}

func TestExtraOptionErrorsDoNotLeakCredentials(t *testing.T) {
	p, err := NewPublisher(PublisherOptions{Options: Options{
		URL: "nats://synthetic-secret@127.0.0.1:1", Topic: "offline", Logger: benchLogger{}, ReportInterval: -1,
		ExtraNatsOptions: []nats.Option{func(*nats.Options) error { return errors.New("invalid credential synthetic-secret") }},
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertNoCredential(t, p.Start())
	p.Stop()
}

func assertNoCredential(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, token := range []string{"synthetic-secret", "synthetic%2Dsecret", "synthetic%ZZsecret", "synthetic-user", "synthetic-password"} {
		if strings.Contains(fmt.Sprintf("%v %+v %s", err, err, err), token) {
			t.Fatal("credential exposed by error formatting")
		}
	}
}

// Event callbacks must redact too; fixing only the Probe/Start return values is not
// enough. Capture the nats Options and invoke the installed callbacks directly,
// using an invalid URL to avoid network access and background reconnects.
type errorCaptureLogger struct{ text strings.Builder }

func (l *errorCaptureLogger) Info(_ string, args ...any)  { fmt.Fprint(&l.text, args...) }
func (l *errorCaptureLogger) Warn(_ string, args ...any)  { fmt.Fprint(&l.text, args...) }
func (l *errorCaptureLogger) Error(_ string, args ...any) { fmt.Fprint(&l.text, args...) }

func TestConnectionCallbacksRedactCredentials(t *testing.T) {
	logger := &errorCaptureLogger{}
	var options *nats.Options
	p, err := NewPublisher(PublisherOptions{Options: Options{
		URL: "nats://synthetic-secret@127.0.0.1:bad", Topic: "offline", Logger: logger, ReportInterval: -1,
		ExtraNatsOptions: []nats.Option{func(o *nats.Options) error { options = o; return nil }},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Start(); err == nil {
		t.Fatal("invalid URL must fail before connecting")
	}
	defer p.Stop()
	cause := errors.New("peer rejected synthetic-secret at nats://synthetic-secret@127.0.0.1:bad")
	options.DisconnectedErrCB(nil, cause)
	options.AsyncErrorCB(nil, nil, cause)
	if strings.Contains(logger.text.String(), "synthetic-secret") {
		t.Fatal("connection callback leaked credential")
	}
}
