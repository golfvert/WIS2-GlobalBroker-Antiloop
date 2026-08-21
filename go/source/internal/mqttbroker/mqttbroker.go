// Package mqttbroker wraps paho.mqtt.golang for the two roles the flow
// has: a Subscriber (centre's own broker, with an optional Backup
// broker to fail over to, and possibly multiple subscribed topics —
// se-smhi subscribes to both an "origin/..." and a "monitor/..." topic
// on the same broker) and a Publisher fan-out (up to 5 global-broker
// targets, each with its own credentials/keepalive).
package mqttbroker

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"antiloop/internal/config"
	"antiloop/internal/gate"
)

// keyLogFileOnce/keyLogWriter back keyLogFile below — a process-wide
// singleton so every TLS connection this process makes (subscriber,
// backup, every pub target) shares ONE open file handle/writer rather
// than each racing to open (and each holding open) its own, which
// would risk interleaved/truncated lines under concurrent writes.
var (
	keyLogFileOnce sync.Once
	keyLogWriter   io.Writer
)

// keyLogFile returns the shared SSLKEYLOGFILE writer for TLS session
// key logging, or nil if SSLKEYLOGFILE isn't set (the overwhelmingly
// common case — this is nil, and every TLS connection behaves exactly
// as before, unless someone explicitly exports SSLKEYLOGFILE before
// starting antiloop).
//
// Unlike curl, OpenSSL-based tools, Node.js, and browsers — which all
// honor this env var automatically — Go's crypto/tls does NOT: an
// application has to explicitly read it and wire tls.Config.
// KeyLogWriter itself, or the env var does nothing at all (confirmed
// live, 2026-08-20: SSLKEYLOGFILE set on antiloop's systemd unit
// produced no file, silently, until this wiring was added).
//
// Purely a debugging aid for otherwise-invisible TLS/WebSocket
// handshake failures — e.g. capturing antiloop's real ClientHello and
// HTTP upgrade request with tcpdump, then decrypting it in Wireshark
// (File -> Preferences -> Protocols -> TLS -> (Pre)-Master-Secret log
// filename) to diff byte-for-byte against a reference client's
// request. Never set in normal operation.
func keyLogFile() io.Writer {
	keyLogFileOnce.Do(func() {
		path := os.Getenv("SSLKEYLOGFILE")
		if path == "" {
			return
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			log.Printf("mqtt: SSLKEYLOGFILE=%q: %v (TLS key logging disabled)", path, err)
			return
		}
		keyLogWriter = f
	})
	return keyLogWriter
}

// EnableLogging wires paho's internal ERROR/CRITICAL/WARN loggers to
// the standard logger. paho defaults to discarding all of these, which
// means real connection failures — bad URL, TLS handshake errors, DNS
// failures, auth rejections — are otherwise completely invisible, on
// top of a worse problem: SetConnectRetry(true) (used below) means a
// per-attempt dial failure does NOT resolve/error the token returned
// by Connect(), it just retries forever in the background. So a
// permanently broken broker (wrong port, bad TLS config, whatever)
// looks identical in our own logs to "still trying to connect" —
// nothing ever prints past the one-time "connecting to..." line.
// Surfacing paho's own internal error is often the only way to see
// why a given broker never actually connects (e.g. a broker URL
// missing a port — see normalizeBrokerURL below).
//
// debug additionally enables paho's DEBUG logger, which logs every
// packet (ping, publish, subscribe ack, ...) — off by default since
// it's very chatty; tied to the same -d flag as message-level logging
// in cmd/antiloop/main.go.
func EnableLogging(debug bool) {
	mqtt.ERROR = log.New(os.Stderr, "[paho ERROR] ", 0)
	mqtt.CRITICAL = log.New(os.Stderr, "[paho CRITICAL] ", 0)
	mqtt.WARN = log.New(os.Stderr, "[paho WARN] ", 0)
	if debug {
		mqtt.DEBUG = log.New(os.Stderr, "[paho DEBUG] ", 0)
	}
}

// normalizeBrokerURL fills in the conventional default port when a
// broker URL doesn't specify one — for mqtt/mqtts/tcp/tcps/ssl/tls
// schemes ONLY. paho.mqtt.golang dials uri.Host verbatim for those
// (see its net.go) and does NOT default a port itself — a URL like
// "mqtts://host" with no port fails at actual dial time with
// something like "address host: missing port in address". Combined
// with SetConnectRetry(true) (see EnableLogging's doc comment), that
// failure was previously invisible, indistinguishable from a slow but
// working connection attempt.
//
// ws/wss are deliberately EXCLUDED from this normalization — fixed
// 2026-08-20 after it caused a real, hard-to-diagnose production bug
// (fr-ifremer-argo, wss://mosquitto.ifremer.fr): for websocket
// targets, paho hands the URL straight to gorilla/websocket's
// Dialer.Dial(), which does its own, separate default-port handling
// in its net/http-facing client.go — critically, it does NOT do this
// uniformly. It sets the HTTP request's Host header directly from
// url.URL.Host, completely unmodified (`Host: u.Host` in its
// DialContext), while only the actual TCP dial address goes through
// its own default-port fallback (hostPortNoPort()). So a bare
// "wss://mosquitto.ifremer.fr" already dials port 443 correctly on
// its own, while sending a clean "Host: mosquitto.ifremer.fr" header
// with no port. Pre-appending ":443" here (as this function used to,
// unconditionally) mutates u.Host BEFORE gorilla ever sees it, so
// that unmodified-Host-header logic faithfully reproduces the port
// right back into the wire request: "Host: mosquitto.ifremer.fr:443".
// Confirmed via a TLS-1.3 keylog + pcap capture, decrypted by hand,
// showing exactly that literal Host header on every antiloop
// connection attempt — and ifremer's Apache frontend doesn't
// recognize it as matching its mosquitto.ifremer.fr vhost, so it
// 301-redirects back to https://mosquitto.ifremer.fr/, silently
// breaking the WebSocket upgrade. curl testing never reproduced this
// because no manual test happened to include a literal ":443" in the
// Host header the way antiloop's own normalization did. Leaving ws/wss
// URLs untouched here lets gorilla's own (correct) split behavior do
// the right thing on both fronts.
//
// If the URL already has a port, or fails to parse, or has no host,
// it's returned unchanged either way — this only ever fills in a
// genuinely missing port on a scheme where doing so is safe, never
// overrides one that's already there.
//
// Defaults, per scheme: mqtt/tcp -> 1883, mqtts/ssl/tls/tcps -> 8883
// (the two plain-MQTT ports). ws/wss are left alone entirely — see
// above.
func normalizeBrokerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Port() != "" {
		return raw
	}
	var defaultPort string
	switch u.Scheme {
	case "mqtts":
		defaultPort = "8883"
	case "ws", "wss":
		return raw // gorilla/websocket handles its own default port — see doc comment above
	default: // "mqtt", "tcp", and anything unrecognized
		defaultPort = "1883"
	}
	u.Host = u.Host + ":" + defaultPort
	return u.String()
}

// ConnState is reported back to the caller so it can drive the
// wmo_wis2_gb_connected_flag / connected_backup_flag /
// monitor_wis2_gb_all_connected_flag gauges — kept out of this package
// so it stays metrics-agnostic.
type ConnState struct {
	Name      string
	Connected bool
}

// Subscriber connects to the centre's primary broker and, if
// configured, fails over to an independent backup broker.
//
// This is active/passive failover with a 60-second debounce in both
// directions, not permanent dual subscription — matching the original
// Node-RED flow's "Manage Backup Broker" node group exactly:
//
//   - A Node-RED `status` node watches the PRIMARY subscriber's
//     connection state (red=disconnected, yellow=connecting,
//     green=connected — red and yellow are treated identically
//     throughout, both wire to the exact same two downstream nodes, so
//     this port only needs a boolean "primary up/down", matching
//     paho's OnConnectHandler / OnConnectionLostHandler exactly).
//   - Two independent 60s Node-RED `trigger` ("Wait") nodes gate the
//     transitions, each cancelable by the OPPOSITE event via a
//     link-wired reset input (trigger nodes' extend:false means a
//     repeat trigger while one is already pending does not restart it
//     — only the first transition after a reset starts the clock):
//
//     primary DOWN (red/yellow):
//       - cancel any pending "disconnect backup" timer
//       - if primary has been seen UP at least once before
//         (seen_green — guards the very first connection race at
//         process startup, before backup could ever be needed): start
//         a 60s "connect backup" timer if one isn't already pending
//
//     primary UP (green):
//       - cancel any pending "connect backup" timer
//       - if this is the very first time ever seeing primary up: just
//         record that (seen_green = true), no backup action
//       - otherwise: start a 60s "disconnect backup" timer if one
//         isn't already pending
//
// So backup is normally fully disconnected. It only comes up after
// primary has been down continuously for a full minute, and only goes
// back down after primary has been healthy continuously for a full
// minute — see backupFailover below, which implements exactly this
// state machine, independent of the MQTT plumbing itself.
type Subscriber struct {
	primary *client

	topics    []string
	qos       byte
	onMessage mqtt.MessageHandler
	onState   func(ConnState)

	backupTarget config.MQTTTarget

	// clientIDBackup/processUUID — activateBackup (below) needs these
	// to build the backup connection's own client id lazily, at
	// activation time, the same way s.primary's was built up front in
	// NewSubscriber — see BuildClientID's doc comment.
	clientIDBackup string
	processUUID    string

	// backup and failover are both nil when no backup is configured
	// (backup.URL == "" at construction) — same as today's behavior in
	// that case. mu guards backup, which is constructed/torn down
	// across the process lifetime as primary flaps, not just once at
	// startup.
	mu       sync.Mutex
	backup   *client
	failover *backupFailover
}

// gbID/clientIDBackup/processUUID feed BuildClientID for the primary
// and backup connections respectively — see that function's doc
// comment. processUUID must be the one shared per-process UUID used
// for every MQTT connection this process makes (subscriber, backup,
// and every publish target alike) — see cmd/antiloop/main.go.
func NewSubscriber(primary, backup config.MQTTTarget, topics []string, qos byte, gbID, clientIDBackup, processUUID string, onMessage mqtt.MessageHandler, onState func(ConnState)) *Subscriber {
	s := &Subscriber{
		topics:         topics,
		qos:            qos,
		onMessage:      onMessage,
		onState:        onState,
		backupTarget:   backup,
		clientIDBackup: clientIDBackup,
		processUUID:    processUUID,
	}

	if backup.URL != "" {
		s.failover = newBackupFailover(s.activateBackup, s.deactivateBackup)
	}

	s.primary = newClient("subscriber", BuildClientID(gbID, processUUID), primary, func(cs ConnState) {
		cs.Name = "primary"
		if onState != nil {
			onState(cs)
		}
		if s.failover != nil {
			if cs.Connected {
				s.failover.primaryUp()
			} else {
				s.failover.primaryDown()
			}
		}
	}, s.subscribeAll)

	return s
}

// subscribeAll (re-)subscribes to every topic in s.topics — shared by
// primary and (lazily, on activation) backup. Has to happen from the
// OnConnect handler, not right after newClient() returns. newClient()
// dials in a background goroutine (Connect() can block for a while,
// especially with SetConnectRetry), so a Subscribe() call made
// immediately after construction almost always races the actual
// connection and fires while the client is still disconnected — paho
// returns an error token for that, which was previously dropped on the
// floor. And even once connected, paho's default CleanSession means
// the broker doesn't remember subscriptions across a reconnect either,
// so this needs to re-run on every reconnect, not just the first one.
// OnConnectHandler fires on both, which is exactly what's needed here.
func (s *Subscriber) subscribeAll(mc mqtt.Client) {
	for _, t := range s.topics {
		token := mc.Subscribe(t, s.qos, s.onMessage)
		token.Wait()
		if err := token.Error(); err != nil {
			log.Printf("mqtt subscribe %q failed: %v", t, err)
		}
	}
}

// activateBackup is backupFailover's connect callback — fired at most
// once per debounce window, from the failover timer's own goroutine,
// never from the MQTT connect/reconnect callbacks directly.
func (s *Subscriber) activateBackup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backup != nil {
		return // already active — shouldn't happen given backupFailover's own pending-timer guard, but don't double-dial
	}
	log.Printf("mqtt [subscriber-backup] primary down for 60s, activating backup broker")
	s.backup = newClient("subscriber-backup", BuildClientID(s.clientIDBackup, s.processUUID), s.backupTarget, func(cs ConnState) {
		cs.Name = "backup"
		if s.onState != nil {
			s.onState(cs)
		}
	}, s.subscribeAll)
}

// deactivateBackup is backupFailover's disconnect callback — fired at
// most once per debounce window.
func (s *Subscriber) deactivateBackup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backup == nil {
		return
	}
	log.Printf("mqtt [subscriber-backup] primary back up for 60s, deactivating backup broker")
	s.backup.close()
	s.backup = nil
	if s.onState != nil {
		s.onState(ConnState{Name: "backup", Connected: false})
	}
}

func (s *Subscriber) Close() {
	s.primary.close()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backup != nil {
		s.backup.close()
	}
}

// backupFailover implements the "Manage Backup Broker" state machine
// from flows.json (see Subscriber's doc comment for the full trace) —
// pure timing/state logic, no MQTT of its own. connectBackup and
// disconnectBackup are called from this type's own timer goroutines,
// never concurrently with each other by construction (each timer is
// stopped before the opposite one can be started).
type backupFailover struct {
	mu sync.Mutex

	seenGreen bool

	// At most one of these is ever non-nil at a time in practice (the
	// opposite transition always stops the other first) — mirrors
	// flows.json's two independent Wait/trigger nodes, each reset by
	// the other's branch.
	connectTimer    *time.Timer
	disconnectTimer *time.Timer

	connectBackup    func()
	disconnectBackup func()
}

func newBackupFailover(connectBackup, disconnectBackup func()) *backupFailover {
	return &backupFailover{connectBackup: connectBackup, disconnectBackup: disconnectBackup}
}

// primaryUp is called on every primary connect (paho's
// OnConnectHandler — fires on the very first connect and on every
// reconnect). Mirrors flows.json's status.fill=="green" branch.
func (f *backupFailover) primaryUp() {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Primary is back — cancel any pending "connect backup" (flows.json:
	// green resets the red/yellow branch's Wait trigger).
	if f.connectTimer != nil {
		f.connectTimer.Stop()
		f.connectTimer = nil
	}

	if !f.seenGreen {
		// First-ever green (startup) — just record it, no backup
		// action. Guards against a spurious backup activation racing
		// the very first primary connection attempt.
		f.seenGreen = true
		return
	}

	if f.disconnectTimer != nil {
		return // already pending — extend:false, don't restart
	}
	f.disconnectTimer = time.AfterFunc(60*time.Second, func() {
		f.mu.Lock()
		f.disconnectTimer = nil
		f.mu.Unlock()
		f.disconnectBackup()
	})
}

// primaryDown is called on every primary disconnect (paho's
// OnConnectionLostHandler). Mirrors flows.json's status.fill=="red"
// and "yellow" branches — treated identically, both wire to the same
// two downstream nodes in the original flow.
func (f *backupFailover) primaryDown() {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Primary dropped again — cancel any pending "disconnect backup"
	// (flows.json: red/yellow resets the green branch's Wait trigger).
	if f.disconnectTimer != nil {
		f.disconnectTimer.Stop()
		f.disconnectTimer = nil
	}

	if !f.seenGreen {
		// Primary has never connected even once yet — nothing to fail
		// over from. Matches flows.json: the red/yellow branch's
		// "Seen Green ?" gate only lets the Wait timer start once
		// seen_green is already true.
		return
	}

	if f.connectTimer != nil {
		return // already pending — extend:false, don't restart
	}
	f.connectTimer = time.AfterFunc(60*time.Second, func() {
		f.mu.Lock()
		f.connectTimer = nil
		f.mu.Unlock()
		f.connectBackup()
	})
}

// Publisher fans a message out to every configured, connected broker —
// matches MQTT_PUB_BROKER..MQTT_PUB_BROKER_5 all being wired in
// parallel in the original flow, each with its own auth/keepalive.
// Publish is best-effort per broker: one broker being down doesn't
// block delivery to the others.
//
// When NONE of the targets are connected, messages are queued (via
// this package's own gate) rather than published-and-failed, then
// flushed in order once any target reconnects — matching the original
// flow's "Monitor connection" group, which wires anyBrokersConnected
// to a q-gate's Open/Queue control input.
//
// The primary mechanism for avoiding lost messages, however, is
// upstream of this package: relay.Pipeline gates entry to its own
// queue on isPrimary && pubConnected jointly (see
// relay.Pipeline.SetPrimary and cmd/antiloop/main.go), which is what
// actually prevents a dedup slot being burned for a message that never
// gets published — the same failure mode internal/gate was built to
// prevent for the primary/secondary role case, just triggered here by
// "no pub broker connected" instead of "not primary". This package's
// own queue is a secondary safety net for the narrow race where a
// broker disconnects between that upstream gate draining a message and
// this Publish() call actually running.
type Publisher struct {
	clients []*client
	mu      sync.RWMutex
	gate    *gate.Gate[publishRequest]
}

type publishRequest struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

// ErrQueued is returned by Publish when no pub broker is currently
// connected — the message was queued (see Publisher's doc comment),
// not delivered or dropped. There is no synchronous delivery result to
// report yet; drainOne logs the eventual outcome itself once the
// backlog flushes.
var ErrQueued = errors.New("mqttbroker: no pub broker connected, message queued for later delivery")

// centreID/processUUID feed BuildClientID — every publish target uses
// the exact same client id (centreID + "_" + the shared per-process
// UUID's own suffix), reproducing the flow's own "Handle connection"
// change node for EVERY MQTT_PUB_BROKER[_2..5] target: all five use the
// literal same jsonata expression, keyed off CENTRE_ID, not a
// per-target variant — see mqttbroker.BuildClientID's doc comment.
// Harmless even though every target ends up with an identical client
// id string: MQTT client-id uniqueness is enforced per-broker, and
// each target is a distinct broker.
func NewPublisher(targets []config.MQTTTarget, centreID, processUUID string, onState func(ConnState)) *Publisher {
	p := &Publisher{}
	// 50000 matches internal/gate's own doc comment on the largest
	// observed q-gate maxQueueLength in flows.json — same convention,
	// same justification, reused here rather than invented fresh.
	p.gate = gate.New(50000, p.drainOne)
	clientID := BuildClientID(centreID, processUUID)
	for i, t := range targets {
		name := fmt.Sprintf("pub-%d", i+1)
		c := newClient(name, clientID, t, func(cs ConnState) {
			cs.Name = name
			if onState != nil {
				onState(cs)
			}
			// Re-evaluate on every connect/disconnect of every target,
			// not just this one — AnyConnected() looks at all of them.
			if p.AnyConnected() {
				p.gate.Open()
			} else {
				p.gate.Queue()
			}
		}, nil) // publishers don't subscribe to anything
		p.mu.Lock()
		p.clients = append(p.clients, c)
		p.mu.Unlock()
	}
	return p
}

// Publish fans out immediately (same best-effort semantics and return
// contract as before) if at least one target is currently connected.
// If none are, the message is queued instead — see Publisher's doc
// comment — and Publish returns (0, ErrQueued) rather than a
// delivered count, since there's nothing to report synchronously yet.
func (p *Publisher) Publish(topic string, qos byte, retained bool, payload []byte) (delivered int, lastErr error) {
	if !p.AnyConnected() {
		p.gate.Handle(publishRequest{topic: topic, qos: qos, retained: retained, payload: payload})
		return 0, ErrQueued
	}
	return p.publishNow(topic, qos, retained, payload)
}

func (p *Publisher) publishNow(topic string, qos byte, retained bool, payload []byte) (delivered int, lastErr error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, c := range p.clients {
		if err := c.publish(topic, qos, retained, payload); err != nil {
			lastErr = err
			continue
		}
		delivered++
	}
	return delivered, lastErr
}

// drainOne is the gate's forward callback — fires once per queued
// message, in order, when Open() drains the backlog after a pub
// broker reconnects. No caller is synchronously waiting on this
// result (the original Publish() call already returned ErrQueued), so
// it logs its own outcome instead of returning one.
func (p *Publisher) drainOne(req publishRequest) {
	delivered, err := p.publishNow(req.topic, req.qos, req.retained, req.payload)
	if err != nil {
		log.Printf("mqtt queued publish to %q: delivered to %d/%d brokers, last error: %v", req.topic, delivered, len(p.clients), err)
	}
}

// AllConnected / AnyConnected mirror the "Connected" function node's
// allBrokersConnected / anyBrokersConnected flow-context booleans.
func (p *Publisher) AllConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.clients) == 0 {
		return false
	}
	for _, c := range p.clients {
		if !c.isConnected() {
			return false
		}
	}
	return true
}

func (p *Publisher) AnyConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, c := range p.clients {
		if c.isConnected() {
			return true
		}
	}
	return false
}

func (p *Publisher) Close() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, c := range p.clients {
		c.close()
	}
}

// --- internal single-client wrapper ---

type client struct {
	mc mqtt.Client
}

// BuildClientID reproduces the flow's own broker.clientid computation
// exactly — each of its three distinct "Handle connection" change
// nodes (primary SUB, backup SUB, every MQTT_PUB_BROKER[_2..5] target)
// builds it the same way: prefix + "_" + the last (12-hex-char)
// segment of one shared, per-process crypto.randomUUID() — but each
// reads a DIFFERENT env var for that prefix: GB_ID for the primary SUB
// connection, MQTT_CLIENTID_BACKUP for the backup SUB connection, and
// CENTRE_ID (always) for every PUB target alike. If prefix is empty —
// only possible for the two SUB variants, since CENTRE_ID is a
// required env var (config.Load fails startup without it) — the flow
// falls back to the bare UUID string as the WHOLE client id,
// unprefixed, matching its own jsonata ternary exactly.
//
// processUUID must be the SAME string across every call for one
// process's entire lifetime (see cmd/antiloop/main.go's single
// uuid.NewString() call) — the flow generates crypto.randomUUID()
// exactly once per process, at startup, and fans that one value out to
// every "Handle connection" node's clientid computation, not once per
// broker connection.
func BuildClientID(prefix, processUUID string) string {
	if prefix == "" {
		return processUUID
	}
	segment := processUUID
	if parts := strings.Split(processUUID, "-"); len(parts) == 5 {
		segment = parts[4]
	}
	return prefix + "_" + segment
}

// onConnect, if non-nil, runs every time the client (re)connects —
// including the very first connection, not just reconnects. Subscribers
// pass one to (re)arm their subscriptions there instead of doing it
// once at construction time (see NewSubscriber's subscribeAll for why).
//
// Every connect attempt, successful connect, disconnect, and initial
// connect failure is logged here unconditionally (clientIDSuffix +
// broker URL identify which of the up-to-7 possible connections —
// subscriber, subscriber-backup, pub-1..pub-5 — it's about), so broker
// connectivity is visible in the process log without needing metrics
// scraped first. This is separate from -d (see cmd/antiloop/main.go),
// which is about individual message traffic, not connection state.
//
// clientIDSuffix is purely a Go-side log tag (has no bearing on the
// actual MQTT client id sent to the broker) — clientID, built by the
// caller via BuildClientID, is what's actually sent on the wire.
func newClient(clientIDSuffix, clientID string, target config.MQTTTarget, onState func(ConnState), onConnect func(mqtt.Client)) *client {
	keepalive := target.Keepalive
	if keepalive == 0 {
		keepalive = 60 * time.Second
	}

	brokerURL := normalizeBrokerURL(target.URL)
	if brokerURL != target.URL {
		log.Printf("mqtt [%s] %s has no port, defaulting to %s", clientIDSuffix, target.URL, brokerURL)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetKeepAlive(keepalive)

	// TLS certificate verification (see config.MQTTTarget.VerifyCert's
	// doc comment) and TLS12Force (see its own doc comment — a Go-only
	// compatibility workaround, not a flow-parity concern) only apply
	// to TLS schemes (mqtts/ssl/tcps/wss); a plain mqtt/ws URL has no
	// TLS handshake to configure in the first place, so this is scoped
	// to not build a pointless tls.Config for those.
	scheme, _, _ := strings.Cut(brokerURL, "://")
	switch scheme {
	case "mqtts", "ssl", "tcps", "wss":
		tlsConfig := &tls.Config{InsecureSkipVerify: !target.VerifyCert}
		if target.TLS12Force {
			tlsConfig.MaxVersion = tls.VersionTLS12
			// MaxVersion alone is not enough: Go's crypto/tls disables
			// every non-forward-secret, RSA-key-exchange cipher suite
			// by default (see crypto/tls's disabledCipherSuites) — that
			// list includes TLS_RSA_WITH_AES_256_CBC_SHA, the exact
			// suite cn-cma's wis2node.wis.cma.cn:8883 was confirmed
			// (via `openssl s_client -tls1_2`, negotiated as
			// "AES256-SHA") to actually support. Without this, a
			// TLS-1.2-capped Go ClientHello still offers only modern
			// ECDHE suites, this broker's legacy stack supports none of
			// them, and the result is the exact same generic
			// "handshake failure" alert as the uncapped case — proven
			// live, 2026-08-20 (MaxVersion cap alone deployed, same
			// failure recurred). Explicitly re-enabling the two RSA-kex
			// CBC-SHA suites, on top of Go's normal secure defaults
			// (kept, not replaced, so this remains safe against any
			// future cn-cma cert/broker rotation that isn't this
			// broken), is required for the handshake to actually
			// succeed.
			cipherSuites := []uint16{tls.TLS_RSA_WITH_AES_256_CBC_SHA, tls.TLS_RSA_WITH_AES_128_CBC_SHA}
			for _, c := range tls.CipherSuites() {
				cipherSuites = append(cipherSuites, c.ID)
			}
			tlsConfig.CipherSuites = cipherSuites
		}
		if w := keyLogFile(); w != nil {
			tlsConfig.KeyLogWriter = w
		}
		opts.SetTLSConfig(tlsConfig)
	}

	opts.SetOnConnectHandler(func(mc mqtt.Client) {
		log.Printf("mqtt [%s] connected to %s", clientIDSuffix, brokerURL)
		if onConnect != nil {
			onConnect(mc)
		}
		if onState != nil {
			onState(ConnState{Connected: true})
		}
	}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("mqtt [%s] disconnected from %s: %v", clientIDSuffix, brokerURL, err)
			if onState != nil {
				onState(ConnState{Connected: false})
			}
		})

	if target.Username != "" {
		opts.SetUsername(target.Username)
	}
	if target.Password != "" {
		opts.SetPassword(target.Password)
	}

	c := &client{mc: mqtt.NewClient(opts)}
	log.Printf("mqtt [%s] connecting to %s", clientIDSuffix, brokerURL)
	go func() {
		if token := c.mc.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("mqtt [%s] connect to %s failed: %v", clientIDSuffix, brokerURL, token.Error())
			if onState != nil {
				onState(ConnState{Connected: false})
			}
		}
	}()
	return c
}

// publishWaitTimeout bounds how long a single publish() call waits on
// paho's token before giving up. Previously an unbounded token.Wait()
// meant a single wedged publish (whatever the root cause) could hang a
// worker goroutine forever with no error, no log line, nothing.
// Bounding it turns a silent permanent hang into a logged, recoverable
// error for that one message — cheap insurance, kept independent of
// the concurrency question below.
const publishWaitTimeout = 10 * time.Second

// publish is intentionally unsynchronized across goroutines for a
// given client: paho.mqtt.golang documents concurrent Publish() calls
// from multiple goroutines on one Client as supported, and forcing
// every publish through a per-client mutex would serialize the
// pipeline's one genuinely network-bound step behind a single broker
// connection regardless of worker count — a real throughput cost for
// no correctness benefit.
func (c *client) publish(topic string, qos byte, retained bool, payload []byte) error {
	token := c.mc.Publish(topic, qos, retained, payload)
	if !token.WaitTimeout(publishWaitTimeout) {
		return fmt.Errorf("publish timed out after %s waiting for paho token (topic=%q)", publishWaitTimeout, topic)
	}
	return token.Error()
}

func (c *client) isConnected() bool {
	return c.mc.IsConnectionOpen()
}

func (c *client) close() {
	c.mc.Disconnect(250)
}
