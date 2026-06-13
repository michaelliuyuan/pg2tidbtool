//go:build integration

// Real-PostgreSQL integration tier for the CDC package.
//
// These tests connect to a LIVE PostgreSQL (>=10, wal_level=logical) and
// exercise the actual logical-replication protocol. They are excluded from the
// default `go test` run by the `integration` build tag, because the mock-based
// unit tests cannot reproduce PG's real protocol behavior.
//
// Run (e.g. against the 23522 source PG):
//
//	CDC_TEST_PG_HOST=h CDC_TEST_PG_PORT=5432 CDC_TEST_PG_USER=u \
//	CDC_TEST_PG_PASSWORD=p CDC_TEST_PG_DATABASE=db CDC_TEST_PG_SSLMODE=disable \
//	go test -tags=integration ./internal/cdc/ -run TestIntegration -v -timeout 60s
//
// This file is the FOUNDATION of the protocol-tier regression gate called for
// after #t48 Bug#4/5/6 — all three slipped past mock unit tests and only failed
// against a real PG (pgoutput framing, START_REPLICATION option syntax, replica
// identity semantics). TODO(tier): extend with a full INSERT/UPDATE/DELETE
// matrix, 'K'/'N' tuple-mapping assertions, and replica-identity modes
// (DEFAULT / FULL / no-PK warning).
package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// integrationPGConfig reads CDC_TEST_PG_* env vars and skips the test when the
// host is unset (so the tier is opt-in and never runs in CI without a PG).
func integrationPGConfig(t *testing.T) (host string, port int, user, password, database, sslmode string) {
	t.Helper()
	host = os.Getenv("CDC_TEST_PG_HOST")
	if host == "" {
		// In CI the tier MUST run — a silent skip here is exactly the
		// "always-green" antipattern this tier exists to prevent (#t48).
		if os.Getenv("CI") != "" {
			t.Fatal("CDC_TEST_PG_HOST unset in CI: integration tier must run, not silently skip")
		}
		t.Skip("CDC_TEST_PG_HOST unset; skipping real-PG integration test (local dev)")
	}
	port, _ = strconv.Atoi(os.Getenv("CDC_TEST_PG_PORT"))
	if port == 0 {
		port = 5432
	}
	user = os.Getenv("CDC_TEST_PG_USER")
	password = os.Getenv("CDC_TEST_PG_PASSWORD")
	database = os.Getenv("CDC_TEST_PG_DATABASE")
	sslmode = os.Getenv("CDC_TEST_PG_SSLMODE")
	if sslmode == "" {
		sslmode = "disable"
	}
	return host, port, user, password, database, sslmode
}

func itRegularDSN(host string, port int, user, password, database, sslmode string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		user, password, host, port, database, sslmode)
}

func itReplicationDSN(host string, port int, user, password, database, sslmode string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s&replication=database",
		user, password, host, port, database, sslmode)
}

// TestIntegration_StartReplicationHandshake is the real-PG regression gate for
// #t48 Bug#6: it runs the actual START_REPLICATION handshake using
// buildPluginArgs() and asserts PG accepts it. With the pre-fix unquoted args
// ("proto_version, 2, ...") PG returns SQLSTATE 42601 here, so this test fails
// on the broken build and passes once the args are quoted `name 'value'`.
func TestIntegration_StartReplicationHandshake(t *testing.T) {
	host, port, user, password, database, sslmode := integrationPGConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Unique per-run slot/publication so repeated/parallel runs don't collide.
	suffix := fmt.Sprintf("itest_%d", time.Now().UnixNano())
	pubName := "pg2tidb_" + suffix
	slotName := "pg2tidb_" + suffix

	// Best-effort cleanup (slot must be inactive -> close replication conn first).
	defer func() {
		cctx, cc := context.WithTimeout(context.Background(), 15*time.Second)
		defer cc()
		if db, err := sql.Open("pgx", itRegularDSN(host, port, user, password, database, sslmode)); err == nil {
			defer db.Close()
			_, _ = db.ExecContext(cctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
			_, _ = db.ExecContext(cctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		}
	}()

	// 1. Create publication (regular connection).
	db, err := sql.Open("pgx", itRegularDSN(host, port, user, password, database, sslmode))
	if err != nil {
		t.Fatalf("open regular conn: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pubName)); err != nil {
		t.Fatalf("create publication %s: %v (does the role have CREATE privilege / is wal_level=logical?)", pubName, err)
	}

	// 2. Replication connection + identify system.
	conn, err := pgconn.Connect(ctx, itReplicationDSN(host, port, user, password, database, sslmode))
	if err != nil {
		t.Fatalf("replication connect: %v", err)
	}
	defer conn.Close(ctx)

	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		t.Fatalf("identify system: %v", err)
	}

	// 3. Create logical slot (ignore "already exists").
	if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{}); err != nil {
		t.Logf("create slot %s (may already exist): %v", slotName, err)
	}

	// 4. THE HANDSHAKE GATE — START_REPLICATION with buildPluginArgs.
	//    Pre-fix this returned SQLSTATE 42601 (unquoted option values).
	if err := pglogrepl.StartReplication(ctx, conn, slotName, sys.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: buildPluginArgs(pubName)}); err != nil {
		t.Fatalf("START_REPLICATION handshake failed — likely Bug#6 regression (unquoted plugin args): %v", err)
	}

	// 5. Confirm the stream is live: try to receive one backend message. Treated
	//    as best-effort (a quiet WAL may time out) — the handshake gate above is
	//    the hard assertion. Receiving/parsing DML events is a tier extension.
	recvCtx, rc := context.WithTimeout(ctx, 5*time.Second)
	defer rc()
	if _, err := conn.ReceiveMessage(recvCtx); err != nil {
		t.Logf("receive first message (best-effort, may time out on quiet WAL): %v", err)
	}
}
