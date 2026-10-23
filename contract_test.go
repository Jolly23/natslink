package natslink

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Both implementations consume this fixture. Expected values describe public
// behavior; no hash value or runtime-specific scheduling detail is serialized.
type sharedContract struct {
	Version    int `json:"version"`
	DedupCases []struct {
		Name     string `json:"name"`
		Entries  int    `json:"entries"`
		WindowMS int    `json:"window_ms"`
		Steps    []struct {
			PayloadHex   *string `json:"payload_hex"`
			Key          *uint64 `json:"key"`
			AdvanceMS    int     `json:"advance_ms"`
			ExpectedSeen bool    `json:"expected_seen"`
		} `json:"steps"`
	} `json:"dedup_cases"`
	SubscriberCases []struct {
		Name      string `json:"name"`
		Entries   int    `json:"entries"`
		CustomKey bool   `json:"custom_key"`
		Steps     []struct {
			PayloadHex string `json:"payload_hex"`
			Subject    string `json:"subject"`
			Gate       bool   `json:"gate"`
			Key        uint64 `json:"key"`
			KeyOK      bool   `json:"key_ok"`
			Expected   string `json:"expected"`
		} `json:"steps"`
	} `json:"subscriber_cases"`
	URLCases []struct {
		Name      string   `json:"name"`
		URL       string   `json:"url"`
		Redacted  string   `json:"redacted"`
		Username  string   `json:"username"`
		Password  *string  `json:"password"`
		Forbidden []string `json:"forbidden"`
	} `json:"url_cases"`
}

func contractPayload(t *testing.T, encoded string) []byte {
	t.Helper()
	payload, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestSharedContract(t *testing.T) {
	f, err := os.Open("testdata/contract-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var contract sharedContract
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		t.Fatal(err)
	}
	if contract.Version != 1 {
		t.Fatalf("unsupported contract version %d", contract.Version)
	}
	for _, tc := range contract.DedupCases {
		t.Run("dedup/"+tc.Name, func(t *testing.T) {
			set := NewDedupSet(tc.Entries, time.Duration(tc.WindowMS)*time.Millisecond)
			for i, step := range tc.Steps {
				if (step.Key == nil) == (step.PayloadHex == nil) {
					t.Fatalf("step %d requires exactly one payload or key", i)
				}
				if step.AdvanceMS > 0 {
					time.Sleep(time.Duration(step.AdvanceMS) * time.Millisecond)
				}
				var seen bool
				if step.Key != nil {
					seen = set.SeenKey(*step.Key)
				} else {
					seen = set.Seen(contractPayload(t, *step.PayloadHex))
				}
				if seen != step.ExpectedSeen {
					t.Fatalf("step %d seen=%v, want %v", i, seen, step.ExpectedSeen)
				}
			}
		})
	}
	for _, tc := range contract.SubscriberCases {
		t.Run("subscriber/"+tc.Name, func(t *testing.T) {
			set := NewDedupSet(tc.Entries, 0)
			var gate, keyOK atomic.Bool
			var key, delivered atomic.Uint64
			subs := make(map[string]*Subscriber)
			for i, step := range tc.Steps {
				s := subs[step.Subject]
				if s == nil {
					o := SubscriberOptions{
						Options: Options{URL: "nats://127.0.0.1:1", Topic: step.Subject, Logger: benchLogger{}, ReportInterval: -1},
						Handler: func(*nats.Msg) { delivered.Add(1) },
						Workers: SyncMode,
						Gate:    gate.Load,
						Dedup:   set,
					}
					if tc.CustomKey {
						o.DedupKey = func(*nats.Msg) (uint64, bool) { return key.Load(), keyOK.Load() }
					}
					var err error
					s, err = NewSubscriber(o)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(s.Stop)
					subs[step.Subject] = s
				}
				gate.Store(step.Gate)
				key.Store(step.Key)
				keyOK.Store(step.KeyOK)
				before, beforeDelivered := s.Stats(), delivered.Load()
				s.dispatch(&nats.Msg{Subject: step.Subject, Data: contractPayload(t, step.PayloadHex)})
				after := s.Stats()
				got := [3]uint64{delivered.Load() - beforeDelivered, after.Gated - before.Gated, after.Deduped - before.Deduped}
				want := map[string][3]uint64{"delivered": {1, 0, 0}, "gated": {0, 1, 0}, "deduped": {0, 0, 1}}
				expected, ok := want[step.Expected]
				if !ok || got != expected || after.Dropped != before.Dropped {
					t.Fatalf("step %d delivered/gated/deduped=%v, want %s (%v); dropped=%d", i, got, step.Expected, expected, after.Dropped)
				}
			}
		})
	}
	for _, tc := range contract.URLCases {
		t.Run("url/"+tc.Name, func(t *testing.T) {
			if got := redactURL(tc.URL); got != tc.Redacted {
				t.Fatalf("redacted=%q, want %q", got, tc.Redacted)
			}
			text := fmt.Sprintf("dial %q: credentials %s", tc.URL, tc.Username)
			if tc.Password != nil {
				text += " / " + *tc.Password
			}
			redacted := redactError(tc.URL, errors.New(text)).Error()
			for _, secret := range tc.Forbidden {
				if strings.Contains(redacted, secret) {
					t.Fatalf("credential escaped redaction: %q", redacted)
				}
			}
			// Capture the real nats.go CONNECT emitted by Probe; testing net/url
			// alone would not establish how credentials reach the NATS client.
			auth := captureContractAuth(t, tc.URL)
			if tc.Password == nil {
				if auth.Token != tc.Username || auth.User != "" || auth.Password != "" {
					t.Fatalf("token CONNECT=%+v, want token %q", auth, tc.Username)
				}
			} else if auth.User != tc.Username || auth.Password != *tc.Password || auth.Token != "" {
				t.Fatalf("user/password CONNECT=%+v, want %q/%q", auth, tc.Username, *tc.Password)
			}
		})
	}
}

type contractAuth struct {
	Token    string `json:"auth_token"`
	User     string `json:"user"`
	Password string `json:"pass"`
}

// This is a bounded protocol stub, not a NATS server. It only captures CONNECT
// and answers the handshake PING; it never accepts a publish or subscription.
func captureContractAuth(t *testing.T, rawURL string) contractAuth {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	type result struct {
		auth contractAuth
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = fmt.Fprint(conn, "INFO {\"server_id\":\"contract-stub\",\"proto\":1,\"auth_required\":true,\"max_payload\":1048576}\r\n")
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		var auth contractAuth
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "CONNECT ") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "CONNECT ")), &auth); err != nil {
					resultCh <- result{err: err}
					return
				}
			} else if line == "PING" {
				_, err := fmt.Fprint(conn, "PONG\r\n")
				resultCh <- result{auth: auth, err: err}
				return
			}
		}
		resultCh <- result{err: fmt.Errorf("handshake ended before PING: %v", scanner.Err())}
	}()
	url := strings.Replace(rawURL, "127.0.0.1:4222", listener.Addr().String(), 1)
	if err := Probe(url); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.auth
	case <-time.After(5 * time.Second):
		t.Fatal("credential handshake timed out")
		return contractAuth{}
	}
}
