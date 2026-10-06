package natslink

import (
	"errors"
	"fmt"
	"strings"
)

// -- Mirroring: apply the same topic's operations to M mutually independent NATS endpoints --
//
// Intended for active-active topologies made of several NATS trees that are
// not connected to each other. The typical case is one machine running two
// local leaf nodes, each with its own upstream: a publish is sent once to each
// endpoint (two ingress paths racing on the sending side), and a subscription
// is made on each endpoint with the callback receiving every copy (the
// receiving side takes the min of two paths). Deduplication always happens on
// the consumer side (hash LRU / sequence-based rejection / pool dedup); the
// mirror itself never judges duplicates.
//
// Mirroring is only a construction mode: the returned types are the ordinary
// ones (*PublisherPool / *Subscriber), every method works the same, and the
// host code is unaware of it except for the New line. Each endpoint is
// supervised and self-healed independently, with no knowledge of the others.
// There is deliberately no cross-endpoint intelligence (no routing, no
// deduplication, no state synchronisation): the whole semantics of a mirror
// is "do it once on every endpoint".
//
// Relation to comma-separated URLs (do not confuse the two): the comma list in
// Options.URL is ordered failover within one endpoint, i.e. only one server is
// connected at a time, in the written order (natslink sets DontRandomize
// explicitly; note that nats.go stays on whichever server it reached and never
// fails back on its own, so this is not full primary/standby). The mirror's
// urls are active-active across endpoints: all are connected, all are used.
// The two are orthogonal and composable: each element of urls may itself be a
// comma list, giving "ordered failover within an endpoint, mirroring across
// endpoints".
//
// Topology discipline (learned the hard way in production): mirroring only
// makes sense when the endpoints sit in front of two independent trees. If
// two local leaf nodes share the same server_name, the upstream treats them
// as the same origin cluster for deduplication / echo suppression, and the
// mirrored copies are collapsed into one or even black-holed against each
// other. The two leaf nodes must have different names.
// For startup fail-fast, call MustProbe once per endpoint URL.

// NewMirroredPublisherPool creates, for each of the M endpoints, a group of n
// publishing connections on the same topic (not yet connected; call Start).
// Connections are named <Name>-e<endpoint index>-<member index>.
// Publish semantics are described in the mirroring paragraph of
// PublisherPool.Publish.
func NewMirroredPublisherPool(urls []string, n int, o PublisherOptions) (*PublisherPool, error) {
	if err := validateMirrorURLs(urls); err != nil {
		return nil, err
	}

	base := o.Options.Name
	pool := &PublisherPool{maxPayload: o.MaxPayload}
	for e, u := range urls {
		eo := o // copy by value; URL/Name are rewritten per endpoint
		eo.Options.URL = u
		eo.Options.Name = mirrorEndpointName(base, "pub", o.Options.Topic, e)
		g, err := newPubGroup(n, eo)
		if err != nil {
			return nil, err
		}
		pool.groups = append(pool.groups, g)
		pool.pubs = append(pool.pubs, g.pubs...)
	}
	return pool, nil
}

// NewMirroredSubscriber creates one subscribing connection per endpoint (not
// yet connected; call Start), all sharing a single worker pool and callback:
// the Handler receives every copy from every endpoint, and deduplication is
// the caller's job; Workers is the total concurrency bound across endpoints.
// Connections are named <Name>-e<endpoint index>.
// Note that SyncMode is no longer serial under a mirror (M delivery
// goroutines call the callback concurrently, see the Handler comment); the
// remaining concurrency / ordering / statistics semantics are documented on
// Handler and Subscriber.Stats.
func NewMirroredSubscriber(urls []string, o SubscriberOptions) (*Subscriber, error) {
	if err := validateMirrorURLs(urls); err != nil {
		return nil, err
	}
	return newSubscriber(urls, o, true)
}

// mirrorEndpointName builds the connection name of endpoint i: the base name
// (falling back to the package's default naming rule) with -e<i> appended, so
// every endpoint can be attributed in logs and server monitoring.
func mirrorEndpointName(base, role, topic string, i int) string {
	if base == "" {
		base = fmt.Sprintf("natslink-%s-%s", role, topic)
	}
	return fmt.Sprintf("%s-e%d", base, i)
}

// validateMirrorURLs fails fast at construction time: at least one endpoint,
// no empty strings, no duplicates. A duplicate endpoint is almost certainly a
// configuration mistake (for several connections to one endpoint, adjust the
// pool size instead of repeating the url).
func validateMirrorURLs(urls []string) error {
	if len(urls) == 0 {
		return errors.New("natslink: mirror requires at least one url")
	}
	seen := make(map[string]struct{}, len(urls))
	for _, u := range urls {
		key := strings.TrimSpace(u)
		if key == "" {
			return errors.New("natslink: mirror url is empty")
		}
		if _, dup := seen[key]; dup {
			return fmt.Errorf("natslink: duplicate mirror url %s", redactURL(u))
		}
		seen[key] = struct{}{}
	}
	return nil
}
