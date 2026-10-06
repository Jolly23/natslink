package natslink

import (
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// safeError preserves errors.Is classification without exposing the
// unredacted original error through Unwrap. An error is only wrapped when its
// text was actually rewritten; the normal publish / delivery paths allocate
// nothing extra.
//
// When the cause chain contains a *url.Error (URL parse failure), urlErr is
// its redacted copy: errors.As for *url.Error yields the copy (Op and the
// underlying classification unchanged, URL masked, Err text masked), never
// the original.
type safeError struct {
	cause   error
	message string
	urlErr  *url.Error
}

func (e *safeError) Error() string        { return e.message }
func (e *safeError) Is(target error) bool { return errors.Is(e.cause, target) }
func (e *safeError) As(target any) bool {
	if t, ok := target.(**url.Error); ok && e.urlErr != nil {
		*t = e.urlErr
		return true
	}
	return false
}

// errorURLPattern matches a URL inside error text up to the next whitespace or
// angle bracket; quotes and backslashes count as part of the URL, and
// redactURL then splits at the last @ to mask the userinfo. net/url renders
// its parse errors with %q of the raw input, so a double quote in the userinfo
// becomes \", a backslash becomes \\, and control characters become \n / \x01;
// a caller applying %q again stacks another layer of escaping, while single
// quotes stay as they are. (An earlier regexp stopped at the first ", never
// reached the @, and leaked the whole credential.) The match is deliberately
// greedy: it swallows a ": that directly follows the URL, but redactURL only
// rewrites the span between the scheme and the last @, so masking too much is
// preferred over parsing escape rules layer by layer.
var errorURLPattern = regexp.MustCompile(`(?:nats|tls|wss?)://[^\s<>]+`)

func redactError(rawURL string, err error) error {
	if err == nil {
		return nil
	}
	rs := redactionFor(rawURL)
	original := err.Error()
	message := original

	// Structured handling first: the text of a *url.Error from a URL parse
	// failure cannot be trusted (%q escaping defeats literal matching), so the
	// whole fragment is replaced with a rendering of the redacted copy, and the
	// text-based fallback runs afterwards.
	var urlErr *url.Error
	if ue := (*url.Error)(nil); errors.As(err, &ue) {
		urlErr = redactURLError(rs, ue)
		message = strings.ReplaceAll(message, ue.Error(), urlErr.Error())
	}
	message = rs.scrub(message)
	if message == original {
		return err
	}
	return &safeError{cause: err, message: message, urlErr: urlErr}
}

// redactURLError rebuilds a *url.Error containing only redacted information:
// Op unchanged, the userinfo masked in URL, and the Err text scrubbed as a
// fallback; a url.EscapeError carries a three-byte fragment of the userinfo
// ("%ZZ"), so it is masked generously.
func redactURLError(rs *redaction, ue *url.Error) *url.Error {
	u := rs.scrub(redactURL(ue.URL))
	var inner error = ue.Err
	if inner != nil {
		text := inner.Error()
		safe := rs.scrub(text)
		var esc url.EscapeError
		if errors.As(inner, &esc) && strings.Contains(ue.URL, "@") {
			safe = `invalid URL escape "***"`
		}
		if safe != text {
			inner = &safeError{cause: inner, message: safe}
		}
	}
	return &url.Error{Op: ue.Op, URL: u, Err: inner}
}

// redaction is the redaction material for one URL, cached per URL (see
// redactionFor).
type redaction struct {
	rawURL    string
	redacted  string   // redactURL(rawURL)
	quotedRaw []string // Go quoted-escape forms of rawURL
	secrets   []string // every form of the credentials, see secretForms
}

// scrub is the text-based fallback: first mask the full URL (verbatim and in
// its Go quoted-escape forms), then mask anything that looks like a URL via
// the regexp, and finally replace every form of the credentials with ***.
func (rs *redaction) scrub(message string) string {
	if rs.rawURL != "" {
		message = strings.ReplaceAll(message, rs.rawURL, rs.redacted)
		for _, q := range rs.quotedRaw {
			message = strings.ReplaceAll(message, q, rs.redacted)
		}
	}
	message = errorURLPattern.ReplaceAllStringFunc(message, redactURL)
	for _, secret := range rs.secrets {
		message = strings.ReplaceAll(message, secret, "***")
	}
	return message
}

// redactionCache caches the redaction material per URL (each link has a fixed
// URL and there are very few of them): during a publish-failure window every
// failed Publish goes through redactError, and rebuilding the map and sorting
// on each call is not acceptable. sync.Map reads are lock-free.
var redactionCache sync.Map // rawURL -> *redaction

func redactionFor(rawURL string) *redaction {
	if v, ok := redactionCache.Load(rawURL); ok {
		return v.(*redaction)
	}
	rs := &redaction{rawURL: rawURL, redacted: redactURL(rawURL), quotedRaw: quotedInner(rawURL), secrets: secretForms(rawURL)}
	redactionCache.Store(rawURL, rs)
	return rs
}

// secretForms lists every form in which the userinfo of each endpoint in
// rawURL could appear in error text: verbatim, percent-decoded, split into
// user / password, and for each of those its Go quoted-escape forms
// (strconv.Quote / QuoteToASCII without the surrounding quotes; wrapped once
// more against a second %q) and percent-encoded forms (PathEscape /
// QueryEscape). There is no minimum length: a short token also masks the same
// characters inside the host as ***, and masking too much is preferred.
// Sorted by length descending so the long forms are replaced first; otherwise
// a short fragment could cut a long form into pieces that slip through.
func secretForms(rawURL string) []string {
	var base []string
	for _, part := range strings.Split(rawURL, ",") {
		part = strings.TrimSpace(part)
		at := strings.LastIndex(part, "@")
		if at < 0 {
			continue
		}
		start := strings.Index(part, "://")
		if start < 0 {
			start = 0
		} else {
			start += 3
		}
		if start > at {
			start = 0
		}
		auth := part[start:at]
		cur := []string{auth}
		if decoded, decodeErr := url.PathUnescape(auth); decodeErr == nil {
			cur = append(cur, decoded)
		}
		for _, secret := range append([]string(nil), cur...) {
			if user, password, ok := strings.Cut(secret, ":"); ok {
				cur = append(cur, user, password)
			}
		}
		base = append(base, cur...)
	}
	seen := make(map[string]struct{})
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	for _, s := range base {
		add(s)
		for _, q := range quotedInner(s) {
			add(q)
			// One more layer of quoted escaping: when a caller applies %q to the
			// error text again (e.g. fmt.Errorf("%q", err.Error())), a single quote
			// cuts the URL regexp short and the double escaping no longer matches
			// the single-layer form. Currently unreachable, but sealed anyway.
			for _, qq := range quotedInner(q) {
				add(qq)
			}
		}
		add(url.PathEscape(s))
		add(url.QueryEscape(s))
	}
	sort.SliceStable(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

// quotedInner returns the Go quoted-escape forms of s (%q / %+q) without the
// surrounding quotes.
func quotedInner(s string) []string {
	q, a := strconv.Quote(s), strconv.QuoteToASCII(s)
	return []string{q[1 : len(q)-1], a[1 : len(a)-1]}
}
