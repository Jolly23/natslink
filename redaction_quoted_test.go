package natslink

// A production incident showed that net/url formats its parse errors with %q of the
// raw URL, so double quotes, backslashes, control characters and non-ASCII bytes in
// the userinfo come out escaped. The old redaction matched the credential literally
// and its URL regexp stopped at the first quote, so both missed: the credential (or a
// recoverable escaped form of it) stayed in the error. All cases use synthetic
// credentials.

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// Each case's secret is assembled from fragments such as Q1..Q4 that never occur in
// normal error text: if any fragment shows up it counts as a leak (including a half
// left over after the secret was split by a quote or an escape).
type leakCase struct {
	name    string
	raw     string
	pieces  []string // raw secret fragments (asserted absent as-is, Go-quoted, ASCII-escaped and percent-encoded)
	errText string   // error classification text that must be preserved
}

var quotedLeakCases = []leakCase{
	{"token-dquote", `nats://Q1tok"Q2tok@127.0.0.1:1`, []string{"Q1tok", "Q2tok"}, "invalid userinfo"},
	{"userpass-dquote", `nats://Q1usr:Q2pa"Q3pa@127.0.0.1:1`, []string{"Q1usr", "Q2pa", "Q3pa"}, "invalid userinfo"},
	{"token-backslash", `nats://Q1bs\Q2bs@127.0.0.1:1`, []string{"Q1bs", "Q2bs"}, "invalid userinfo"},
	{"userpass-backslash", `nats://Q1u:Q2p\"Q3p@127.0.0.1:1`, []string{"Q1u", "Q2p", "Q3p"}, "invalid userinfo"},
	{"token-newline", "nats://Q1nl\nQ2nl@127.0.0.1:1", []string{"Q1nl", "Q2nl"}, "invalid control character"},
	{"token-ctrl", "nats://Q1cc\x01Q2cc@127.0.0.1:1", []string{"Q1cc", "Q2cc"}, "invalid control character"},
	{"token-tab-dquote", "nats://Q1tb\t\"Q2tb@127.0.0.1:1", []string{"Q1tb", "Q2tb"}, "invalid control character"},
	{"token-nonascii", `nats://Q1éQ2na"x@127.0.0.1:1`, []string{"Q1é", "Q2na"}, "invalid userinfo"},
	{"token-pct-dquote", `nats://Q1pc%22Q2pc"@127.0.0.1:1`, []string{"Q1pc", "Q2pc"}, "invalid userinfo"},
	{"token-bad-escape", `nats://Q1be%ZZQ2be@127.0.0.1:4222`, []string{"Q1be", "Q2be"}, "invalid URL escape"},
	{"comma-list-dquote", `nats://127.0.0.1:1,nats://Q1cl"Q2cl@127.0.0.1:2`, []string{"Q1cl", "Q2cl"}, "invalid userinfo"},
}

// leakForms lists every form in which a secret fragment could appear in error text.
func leakForms(s string) []string {
	forms := []string{s, url.PathEscape(s), url.QueryEscape(s)}
	for _, q := range []string{strconv.Quote(s), strconv.QuoteToASCII(s)} {
		forms = append(forms, q[1:len(q)-1])
	}
	return forms
}

// errorTexts collects every formatted rendering of the error and of its whole Unwrap
// chain (including Unwrap() []error).
func errorTexts(err error) []string {
	var out []string
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		out = append(out, e.Error(), fmt.Sprintf("%v|%+v|%s|%q|%#v", e, e, e, e, e))
		switch u := e.(type) {
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		case interface{ Unwrap() []error }:
			for _, x := range u.Unwrap() {
				walk(x)
			}
		}
	}
	walk(err)
	var ue *url.Error
	if errors.As(err, &ue) {
		out = append(out, ue.Error(), ue.URL, ue.Err.Error(), fmt.Sprintf("%#v", ue))
	}
	return out
}

func assertNoLeak(t *testing.T, tc leakCase, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error", tc.name)
	}
	for _, text := range errorTexts(err) {
		for _, p := range tc.pieces {
			for _, f := range leakForms(p) {
				if strings.Contains(text, f) {
					t.Fatalf("%s: secret piece %q (form %q) leaked in %q", tc.name, p, f, text)
				}
			}
		}
	}
	if !strings.Contains(err.Error(), tc.errText) {
		t.Fatalf("%s: lost error classification %q: %q", tc.name, tc.errText, err.Error())
	}
}

// Double %q: the url.Error text gets %q-formatted once more by a caller. A single
// quote makes the URL regexp stop early, and the doubly escaped form is not covered
// by single-level quote escaping. Unreachable on the current call path (nothing %q's
// the error before redactError), but sealed anyway.
func TestDoubleQuotedErrorTextDoesNotLeak(t *testing.T) {
	tc := leakCase{"double-quoted", `nats://Q1dq'Q2dq"Q3dq@127.0.0.1:1`, []string{"Q1dq", "Q2dq", "Q3dq"}, "invalid userinfo"}
	_, perr := url.Parse(tc.raw)
	if perr == nil {
		t.Fatal("url.Parse unexpectedly succeeded")
	}
	once := fmt.Sprintf("%q", perr.Error())
	for _, text := range []string{once, fmt.Sprintf("%q", once), fmt.Sprintf("%+q", once)} {
		assertNoLeak(t, tc, redactError(tc.raw, errors.New("wrapped: "+text)))
	}
}

func TestQuotedURLErrorsDoNotLeakCredentials(t *testing.T) {
	for _, tc := range quotedLeakCases {
		t.Run(tc.name, func(t *testing.T) {
			assertNoLeak(t, tc, Probe(tc.raw))

			p, err := NewPublisher(PublisherOptions{Options: Options{URL: tc.raw, Topic: "offline", Logger: benchLogger{}, ReportInterval: -1}})
			if err != nil {
				t.Fatal(err)
			}
			err = p.Start()
			p.Stop()
			assertNoLeak(t, tc, err)

			// Feed a url.Parse error directly (with nested wrapping): the structured
			// classification stays reachable, and the copy reached is redacted too.
			// nats.go splits a comma list and parses each part; take the part that
			// carries the userinfo.
			part := tc.raw
			if i := strings.LastIndex(part, ","); i >= 0 {
				part = part[i+1:]
			}
			_, perr := url.Parse(part)
			if perr == nil {
				t.Fatalf("url.Parse(%s) unexpectedly succeeded", tc.name)
			}
			red := redactError(tc.raw, fmt.Errorf("outer: %w", perr))
			assertNoLeak(t, tc, red)
			var ue *url.Error
			if !errors.As(red, &ue) {
				t.Fatalf("%s: redacted error no longer exposes *url.Error", tc.name)
			}
			if ue.Op != "parse" || !strings.Contains(ue.Err.Error(), tc.errText) {
				t.Fatalf("%s: url.Error classification lost: %#v", tc.name, ue)
			}
		})
	}
}
