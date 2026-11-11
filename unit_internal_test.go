package bullmq

// Ports of the #[cfg(test)] unit tests of bullmq_rust/src/{keys,error,types,
// queue,scripts,redis_connection,worker}.rs (options.rs has none), adapted to
// the Go equivalents.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/imdat99/bullmq/internal/scripts"
)

// ═══════════════════════════════════════════════════════════════════════════
// keys.rs
// ═══════════════════════════════════════════════════════════════════════════

func mxEq(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func mxIsInvalidConfig(t *testing.T, err error, input string) {
	t.Helper()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("%q: expected ErrInvalidConfig, got %v", input, err)
	}
}

func TestDefaultPrefix(t *testing.T) {
	keys := NewQueueKeys("test-queue", "")
	mxEq(t, keys.Base(), "bull:test-queue")
	mxEq(t, keys.Wait(), "bull:test-queue:wait")
	mxEq(t, keys.Active(), "bull:test-queue:active")
	mxEq(t, keys.Meta(), "bull:test-queue:meta")
}

func TestCustomPrefix(t *testing.T) {
	keys := NewQueueKeys("my-queue", "myapp")
	mxEq(t, keys.Base(), "myapp:my-queue")
	mxEq(t, keys.Delayed(), "myapp:my-queue:delayed")
}

// Rust tests its base64_standard helper; Go uses encoding/base64 inside
// QueueKeys.ClientName, so the encoding is checked through it.
func TestBase64StandardMatchesNode(t *testing.T) {
	// Equivalent to Node `Buffer.from(s).toString('base64')`.
	for name, want := range map[string]string{
		"test":     "dGVzdA==",
		"my-queue": "bXktcXVldWU=",
		"f":        "Zg==",
		"fo":       "Zm8=",
		"foo":      "Zm9v",
		"":         "",
	} {
		mxEq(t, NewQueueKeys(name, "bull").ClientName(""), "bull:"+want)
	}
}

func TestClientName(t *testing.T) {
	keys := NewQueueKeys("test", "bull")
	mxEq(t, keys.ClientName(""), "bull:dGVzdA==")
	mxEq(t, keys.ClientName(":w:worker-1"), "bull:dGVzdA==:w:worker-1")
}

func TestJobKey(t *testing.T) {
	keys := NewQueueKeys("q", "")
	mxEq(t, keys.JobKey("123"), "bull:q:123")
}

func TestResolvesUnqualifiedParentQueueNames(t *testing.T) {
	got, err := resolveParentQueueKey("bull", "parent-queue")
	if err != nil {
		t.Fatal(err)
	}
	mxEq(t, got, "bull:parent-queue")
}

func TestAcceptsPrequalifiedParentQueueKeysForCurrentPrefix(t *testing.T) {
	got, err := resolveParentQueueKey("bull", "bull:parent-queue")
	if err != nil {
		t.Fatal(err)
	}
	mxEq(t, got, "bull:parent-queue")
}

func TestRejectsParentQueueNamesWithExtraColons(t *testing.T) {
	for _, q := range []string{"parent:queue", "bull:parent:queue"} {
		_, err := resolveParentQueueKey("bull", q)
		mxIsInvalidConfig(t, err, q)
	}
}

func TestAcceptsCustomJobIdsWithoutColons(t *testing.T) {
	for _, id := range []string{"job-1", ""} {
		if err := validateCustomJobID(id); err != nil {
			t.Fatalf("%q: %v", id, err)
		}
	}
}

func TestAcceptsPlainPrefixes(t *testing.T) {
	for _, p := range []string{"bull", "{bull}", "my-app_1"} {
		if err := validatePrefix(p); err != nil {
			t.Fatalf("%q: %v", p, err)
		}
	}
}

func TestRejectsEmptyOrColonContainingPrefixes(t *testing.T) {
	for _, prefix := range []string{"", "tenant:region", ":bull", "bull:"} {
		mxIsInvalidConfig(t, validatePrefix(prefix), prefix)
	}
}

func TestAcceptsLegacyThreeSegmentRepeatableJobIds(t *testing.T) {
	if err := validateCustomJobID("repeat:scheduler-id:1700000000000"); err != nil {
		t.Fatal(err)
	}
}

func TestRejectsAmbiguousCustomJobIdsWithColons(t *testing.T) {
	for _, id := range []string{"job:1", "a:b:c:d"} {
		mxIsInvalidConfig(t, validateCustomJobID(id), id)
	}
}

func TestRejectsIntegerCustomJobIds(t *testing.T) {
	for _, id := range []string{"100", "0", "-5", "9007199254740992"} {
		err := validateCustomJobID(id)
		mxIsInvalidConfig(t, err, id)
		if !strings.HasSuffix(err.Error(), ": Custom Id cannot be integers") {
			t.Fatalf("%q: got %v", id, err)
		}
	}
}

func TestAcceptsNonCanonicalIntegerLikeCustomJobIds(t *testing.T) {
	// Matches Node `` `${parseInt(id, 10)}` === id ``, which only rejects the
	// canonical decimal form.
	for _, id := range []string{"007", "+1", "1.5", "1a", " 1", "-0"} {
		if err := validateCustomJobID(id); err != nil {
			t.Fatalf("expected %q to be accepted: %v", id, err)
		}
	}
}

func TestAcceptsIntegerIdsOutsideTheSafeDoubleRange(t *testing.T) {
	// `parseInt` goes through an IEEE-754 double, so these do not survive
	// the round trip in Node either:
	//   parseInt('9007199254740993')      -> 9007199254740992
	//   parseInt('-9223372036854775808')  -> -9223372036854776000
	for _, id := range []string{
		"9007199254740993",
		"9223372036854775808",
		"-9223372036854775808",
		"1000000000000000000000",
	} {
		if err := validateCustomJobID(id); err != nil {
			t.Fatalf("expected %q to be accepted: %v", id, err)
		}
	}
}

func TestRejectsNanLikeNodeDoes(t *testing.T) {
	// Quirk of the Node check being emulated: parseInt('NaN') is NaN and
	// `${NaN}` === 'NaN', so Node reports it as an integer id.
	err := validateCustomJobID("NaN")
	mxIsInvalidConfig(t, err, "NaN")
	if !strings.HasSuffix(err.Error(), ": Custom Id cannot be integers") {
		t.Fatalf("got %v", err)
	}
	// 'Infinity' is not affected: parseInt stops before any digit.
	if err := validateCustomJobID("Infinity"); err != nil {
		t.Fatal(err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// error.rs
// ═══════════════════════════════════════════════════════════════════════════

func TestMapsMissingLockCode(t *testing.T) {
	err := NewScriptError(CodeJobLockNotExist)
	mxEq(t, err.Code, int64(-2))
	mxEq(t, err.Message, "Job lock does not exist")
}

func TestMapsLockMismatchCode(t *testing.T) {
	err := NewScriptError(CodeJobLockMismatch)
	mxEq(t, err.Code, int64(-6))
	mxEq(t, err.Message, "Job lock mismatch")
}

// ═══════════════════════════════════════════════════════════════════════════
// types.rs
// ═══════════════════════════════════════════════════════════════════════════

func TestBackoffStrategySerialization(t *testing.T) {
	custom := CustomBackoff("myType")
	raw, err := custom.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	if !strings.Contains(js, `"type":"myType"`) || !strings.Contains(js, `"delay":0`) {
		t.Fatalf("custom json = %s", js)
	}

	fixed := FixedBackoff(1000 * time.Millisecond)
	raw2, err := fixed.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	js2 := string(raw2)
	if !strings.Contains(js2, `"type":"fixed"`) || !strings.Contains(js2, `"delay":1000`) {
		t.Fatalf("fixed json = %s", js2)
	}

	// Test round-trip for custom
	var parsed BackoffStrategy
	if err := parsed.UnmarshalJSON(raw); err != nil {
		t.Fatal(err)
	}
	if !parsed.IsCustom() || parsed.Type != "myType" {
		t.Fatalf("parsed = %+v", parsed)
	}

	// Test deserialization of what Lua stores (type=custom name, delay=number)
	var parsed2 BackoffStrategy
	if err := parsed2.UnmarshalJSON([]byte(`{"type":"myCustom","delay":500}`)); err != nil {
		t.Fatal(err)
	}
	if !parsed2.IsCustom() || parsed2.Type != "myCustom" {
		t.Fatalf("parsed2 = %+v", parsed2)
	}

	// Test deserialization of fixed
	var parsed3 BackoffStrategy
	if err := parsed3.UnmarshalJSON([]byte(`{"type":"fixed","delay":1000}`)); err != nil {
		t.Fatal(err)
	}
	mxEq(t, parsed3, *FixedBackoff(1000 * time.Millisecond))
}

// ═══════════════════════════════════════════════════════════════════════════
// queue.rs — client_list_tests
// ═══════════════════════════════════════════════════════════════════════════

func TestMatchesUnnamedAndNamedWorkers(t *testing.T) {
	unnamed := "bull:dGVzdA=="
	namedPrefix := "bull:dGVzdA==:w:"
	list := fmt.Sprintf("id=1 addr=127.0.0.1:1 name=%s age=5\n"+
		"id=2 addr=127.0.0.1:2 name=%salpha age=3\n"+
		"id=3 addr=127.0.0.1:3 name=bull:b3RoZXI= age=1\n"+
		"id=4 addr=127.0.0.1:4 name= age=1\n", unnamed, namedPrefix)

	clients := parseClientList(list, "test", unnamed, namedPrefix)
	if len(clients) != 2 {
		t.Fatalf("clients = %v, want 2", clients)
	}
	mxEq(t, clients[0]["name"], "test")
	mxEq(t, clients[0]["rawname"], unnamed)
	mxEq(t, clients[1]["rawname"], namedPrefix+"alpha")
	mxEq(t, clients[1]["name"], "test")
}

func TestReturnsEmptyWhenNoMatch(t *testing.T) {
	list := "id=1 addr=127.0.0.1:1 name=other age=5\n"
	if clients := parseClientList(list, "test", "bull:dGVzdA==", "bull:dGVzdA==:w:"); len(clients) != 0 {
		t.Fatalf("clients = %v, want empty", clients)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// queue.rs — prometheus_tests
// ═══════════════════════════════════════════════════════════════════════════

func TestEscapesSpecialCharacters(t *testing.T) {
	mxEq(t, escapePrometheusLabelValue("plain"), "plain")
	mxEq(t, escapePrometheusLabelValue("a\\b"), "a\\\\b")
	mxEq(t, escapePrometheusLabelValue("a\"b"), "a\\\"b")
	mxEq(t, escapePrometheusLabelValue("a\nb"), "a\\nb")
}

// ═══════════════════════════════════════════════════════════════════════════
// queue.rs — progress_serialization_tests
// ═══════════════════════════════════════════════════════════════════════════

func TestSerializesNonFiniteNumbersAsNull(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		raw, err := marshalProgress(v)
		if err != nil {
			t.Fatal(err)
		}
		mxEq(t, string(raw), "null")
	}
}

func TestSerializesRegularValuesAsJson(t *testing.T) {
	raw, err := marshalProgress(42.0)
	if err != nil {
		t.Fatal(err)
	}
	// Rust serde prints "42.0"; Go encoding/json prints "42", which is what
	// Node.js JSON.stringify(42) produces.
	mxEq(t, string(raw), "42")
}

// ═══════════════════════════════════════════════════════════════════════════
// queue.rs — dependency_key_tests
// ═══════════════════════════════════════════════════════════════════════════

func mxSplit(prefix, queueName, jobID string) (splitJobKey, bool) {
	return splitJobKey{prefix: prefix, queueName: queueName, jobID: jobID}, true
}

func mxAssertSplit(t *testing.T, key string, want splitJobKey, wantOK bool) {
	t.Helper()
	got, ok := splitQualifiedJobKey(key)
	if ok != wantOK || got != want {
		t.Fatalf("splitQualifiedJobKey(%q) = %+v, %v; want %+v, %v", key, got, ok, want, wantOK)
	}
}

func TestSplitsPlainKeys(t *testing.T) {
	w, ok := mxSplit("bull", "queue", "1")
	mxAssertSplit(t, "bull:queue:1", w, ok)
}

func TestPreservesColonsInCustomJobIds(t *testing.T) {
	// Prefixes and queue names cannot contain `:`, so everything after the
	// second separator belongs to the id — no stored `opts.jobId` or Redis
	// probe is needed to find the boundary.
	w, ok := mxSplit("bull", "queue", "repeat:sched:1700000000000")
	mxAssertSplit(t, "bull:queue:repeat:sched:1700000000000", w, ok)
	w, ok = mxSplit("bull", "queue", "a:b:c:d")
	mxAssertSplit(t, "bull:queue:a:b:c:d", w, ok)
}

func TestSplitsChildrenStoredUnderAForeignPrefix(t *testing.T) {
	// The boundary comes from the key itself, so a child living under a
	// prefix other than the parent queue's resolves correctly, colon id or
	// not.
	w, ok := mxSplit("tenant", "child", "1")
	mxAssertSplit(t, "tenant:child:1", w, ok)
	w, ok = mxSplit("tenant", "child", "repeat:sched:1")
	mxAssertSplit(t, "tenant:child:repeat:sched:1", w, ok)
}

func TestRejectsKeysWithoutAPrefixQueueNameOrJobId(t *testing.T) {
	for _, key := range []string{":queue:1", "bull::1", "bull:queue:", "queue:1"} {
		mxAssertSplit(t, key, splitJobKey{}, false)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// scripts.rs
// ═══════════════════════════════════════════════════════════════════════════

func mxRedisURL() string {
	if u := os.Getenv("REDIS_URL"); u != "" {
		return u
	}
	return "redis://127.0.0.1:6379"
}

func mxRawClient(t *testing.T) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(mxRedisURL())
	if err != nil {
		t.Fatalf("valid REDIS_URL: %v", err)
	}
	c := redis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSha1Computation(t *testing.T) {
	sha := redis.NewScript("return 1").Hash()
	mxEq(t, len(sha), 40)
}

func TestExecuteFallsBackToEvalAndCachesScript(t *testing.T) {
	ctx := context.Background()
	conn := mxRawClient(t)
	if err := conn.Do(ctx, "SCRIPT", "FLUSH", "SYNC").Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}

	script := scripts.New("test", 0, "return 1")
	result, err := script.Run(ctx, conn, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	mxEq(t, result, any(int64(1)))

	exists, err := conn.ScriptExists(ctx, script.Hash()).Result()
	if err != nil {
		t.Fatalf("SCRIPT EXISTS: %v", err)
	}
	mxEq(t, exists, []bool{true})
}

// Rust: ScriptRegistry::ensure_loaded(["addStandardJob", "addStandardJob"]);
// Go: scripts.Script.Load on the requested scripts.
func TestEnsureLoadedLoadsOnlyRequestedScripts(t *testing.T) {
	ctx := context.Background()
	conn := mxRawClient(t)
	if err := conn.Do(ctx, "SCRIPT", "FLUSH", "SYNC").Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}

	registry, err := scripts.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"addStandardJob", "addStandardJob"} {
		if err := registry.Get(name).Load(ctx, conn); err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
	}

	addStandard := registry.Get("addStandardJob")
	addParent := registry.Get("addParentJob")
	existing, err := conn.ScriptExists(ctx, addStandard.Hash(), addParent.Hash()).Result()
	if err != nil {
		t.Fatalf("SCRIPT EXISTS: %v", err)
	}
	mxEq(t, existing, []bool{true, false})
}

// ═══════════════════════════════════════════════════════════════════════════
// redis_connection.rs
// ═══════════════════════════════════════════════════════════════════════════

// Rust build_client / build_cluster_client → Go newUniversalClient (which
// builds the TLS config with buildTLSConfig) without connecting.
func mxBuildClient(t *testing.T, opts RedisConnectionOptions) error {
	t.Helper()
	c, err := newUniversalClient(opts, "")
	if c != nil {
		_ = c.Close()
	}
	return err
}

func mxOptsWithCerts(certs TLSCerts) RedisConnectionOptions {
	return RedisConnectionOptions{TLSCerts: &certs}
}

func TestBuildClientWithoutTlsCertsSucceeds(t *testing.T) {
	if err := mxBuildClient(t, RedisConnectionOptions{URL: "redis://127.0.0.1:6379"}); err != nil {
		t.Fatal(err)
	}
}

// Go validates PEM material when the client is built (fail fast) whereas
// redis-rs only fails on connect, so the success cases use a real
// self-signed certificate and invalid PEM is rejected up front.
func TestBuildClientWithRootCertOnlySucceeds(t *testing.T) {
	opts := mxOptsWithCerts(TLSCerts{RootCert: mxSelfSignedPEM(t)})
	opts.URL = "rediss://127.0.0.1:6379"
	if err := mxBuildClient(t, opts); err != nil {
		t.Fatal(err)
	}
}

func TestBuildClientWithClientCertWithoutKeyErrors(t *testing.T) {
	opts := mxOptsWithCerts(TLSCerts{ClientCert: []byte("cert")})
	opts.URL = "rediss://127.0.0.1:6379"
	mxIsInvalidConfig(t, mxBuildClient(t, opts), "client cert without key")
}

func TestBuildClientWithClientKeyWithoutCertErrors(t *testing.T) {
	opts := mxOptsWithCerts(TLSCerts{ClientKey: []byte("key")})
	opts.URL = "rediss://127.0.0.1:6379"
	mxIsInvalidConfig(t, mxBuildClient(t, opts), "client key without cert")
}

func mxClusterOpts() RedisConnectionOptions {
	return RedisConnectionOptions{ClusterNodes: []string{"redis://127.0.0.1:7000", "redis://127.0.0.1:7001"}}
}

func TestBuildClusterClientSucceeds(t *testing.T) {
	if err := mxBuildClient(t, mxClusterOpts()); err != nil {
		t.Fatal(err)
	}
}

func TestBuildClusterClientWithTlsCertsSucceeds(t *testing.T) {
	opts := mxClusterOpts()
	opts.TLSCerts = &TLSCerts{RootCert: mxSelfSignedPEM(t)}
	if err := mxBuildClient(t, opts); err != nil {
		t.Fatal(err)
	}
}

func TestBuildClientRejectsInvalidRootCertPEM(t *testing.T) {
	opts := mxOptsWithCerts(TLSCerts{RootCert: []byte("cert")})
	opts.URL = "rediss://127.0.0.1:6379"
	mxIsInvalidConfig(t, mxBuildClient(t, opts), "invalid root cert PEM")
}

// mxSelfSignedPEM returns a freshly generated self-signed CA certificate.
func mxSelfSignedPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bullmq-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestBuildClusterClientWithClientCertWithoutKeyErrors(t *testing.T) {
	opts := mxClusterOpts()
	opts.TLSCerts = &TLSCerts{ClientCert: []byte("cert")}
	mxIsInvalidConfig(t, mxBuildClient(t, opts), "cluster client cert without key")
}

func TestRedactsUsernamePassword(t *testing.T) {
	mxEq(t, redactURLUserinfo("redis://user:pass@localhost:6379/0"), "redis://***@localhost:6379/0")
}

func TestRedactsPasswordOnlyAndKeepsIpv6Host(t *testing.T) {
	mxEq(t, redactURLUserinfo("rediss://:p%40ss@[::1]:6380/2"), "rediss://***@[::1]:6380/2")
}

func TestKeepsUrlWithoutUserinfo(t *testing.T) {
	input := "redis://localhost:6379/0"
	mxEq(t, redactURLUserinfo(input), input)
}

// ═══════════════════════════════════════════════════════════════════════════
// worker.rs
// ═══════════════════════════════════════════════════════════════════════════

func TestDiscardPreservesWorkerControlFlowErrors(t *testing.T) {
	if shouldConvertDiscardedError(ErrDelayed) {
		t.Fatal("ErrDelayed must not be converted")
	}
	if shouldConvertDiscardedError(ErrWaitingChildren) {
		t.Fatal("ErrWaitingChildren must not be converted")
	}
	if shouldConvertDiscardedError(NewRateLimitError(100 * time.Millisecond)) {
		t.Fatal("RateLimitError must not be converted")
	}
}

func TestDiscardConvertsRealFailuresToUnrecoverable(t *testing.T) {
	if !shouldConvertDiscardedError(errors.New("boom")) {
		t.Fatal("processing error must be converted")
	}
	// Rust: Error::Redis(Io, "boom").
	if !shouldConvertDiscardedError(fmt.Errorf("boom: %w", io.ErrUnexpectedEOF)) {
		t.Fatal("redis I/O error must be converted")
	}
	if shouldConvertDiscardedError(NewUnrecoverableError("fatal")) {
		t.Fatal("unrecoverable error must not be converted")
	}
}
