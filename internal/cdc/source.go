package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
)

// Source connects to a PostgreSQL database and streams logical replication
// changes via the pgoutput plugin.
type Source struct {
	cfg    SourceConfig
	log    *zap.Logger
	db     *sql.DB // regular connection for setup (create slot/pub)

	mu        sync.Mutex
	conn      *pgconn.PgConn // dedicated replication connection
	relations map[uint32]*Relation // relation OID → schema info
	running   bool
	stopCh    chan struct{}

	// Metrics
	eventsReceived int64
	lsnCurrent     pglogrepl.LSN
}

// Relation holds table metadata learned from the replication stream.
type Relation struct {
	OID       uint32
	Schema    string
	Name      string
	Columns   []RelationColumn
}

// RelationColumn is a single column in a replicated table.
type RelationColumn struct {
	Name      string
	TypeOID   uint32
	TypeName  string
	IsKey     bool
	Ordinal   int
}

// NewSource creates a new CDC Source.
func NewSource(cfg SourceConfig) *Source {
	if cfg.OutputPlugin == "" {
		cfg.OutputPlugin = "pgoutput"
	}
	if cfg.SlotName == "" {
		cfg.SlotName = "pg2tidb_cdc"
	}
	if cfg.Publication == "" {
		cfg.Publication = "pg2tidb_pub"
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	return &Source{
		cfg:       cfg,
		log:       zap.NewNop(),
		relations: make(map[uint32]*Relation),
		stopCh:    make(chan struct{}),
	}
}

// SetLogger sets the logger for the source.
func (s *Source) SetLogger(log *zap.Logger) {
	s.log = log
}

// dsn returns a PostgreSQL connection string without specifying a database
// (used for the replication connection which connects to the specific DB).
func (s *Source) dsn() string {
	sslmode := s.cfg.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	return fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s?sslmode=%s&replication=database",
		s.cfg.User, s.cfg.Password, s.cfg.Host, s.cfg.Port, s.cfg.Database, sslmode,
	)
}

// regularDSN returns a normal connection string for setup queries.
func (s *Source) regularDSN() string {
	sslmode := s.cfg.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	return fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		s.cfg.User, s.cfg.Password, s.cfg.Host, s.cfg.Port, s.cfg.Database, sslmode,
	)
}

// Setup creates the replication slot and publication if they don't exist.
// Call this once before Start().
func (s *Source) Setup(ctx context.Context) error {
	db, err := sql.Open("pgx", s.regularDSN())
	if err != nil {
		return fmt.Errorf("cdc setup: connect: %w", err)
	}
	defer db.Close()
	s.db = db

	// Create publication
	pubSQL := fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, quoteIdent(s.cfg.Publication))
	_, err = db.ExecContext(ctx, pubSQL)
	if err != nil {
		// Ignore "already exists" error
		s.log.Debug("create publication (may already exist)", zap.Error(err))
	}

	// Create replication slot
	// This requires a replication connection, not a regular one
	// We defer this to Start() where we have the replication conn
	s.log.Info("cdc setup complete",
		zap.String("publication", s.cfg.Publication),
		zap.String("slot", s.cfg.SlotName),
	)
	return nil
}

// Start begins streaming logical replication changes from PG.
// It creates the replication connection, starts the slot, and begins consuming.
func (s *Source) Start(ctx context.Context, startLSN pglogrepl.LSN) (<-chan *CDCEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil, fmt.Errorf("cdc source: already running")
	}

	// Establish replication connection
	conn, err := pgconn.Connect(ctx, s.dsn())
	if err != nil {
		return nil, fmt.Errorf("cdc source: replication connect: %w", err)
	}
	s.conn = conn

	// Identify system
	sysIdent, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("cdc source: identify system: %w", err)
	}
	s.log.Info("cdc identified system",
		zap.String("system_id", sysIdent.SystemID),
		zap.Int32("timeline", sysIdent.Timeline),
		zap.String("xlogpos", sysIdent.XLogPos.String()),
		zap.String("dbname", sysIdent.DBName),
	)

	// Create replication slot (if LSN is 0, use the current WAL position)
	slotLSN := startLSN
	if slotLSN == 0 {
		// Create the slot at the current WAL position
		slotLSN = sysIdent.XLogPos
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, s.cfg.SlotName, s.cfg.OutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		s.log.Debug("create replication slot (may already exist)", zap.Error(err))
	}

	// Start replication
	pluginArgs := []string{
		"proto_version", "2",
		"publication_names", s.cfg.Publication,
	}
	err = pglogrepl.StartReplication(ctx, conn, s.cfg.SlotName, slotLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
		})
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("cdc source: start replication: %w", err)
	}

	s.running = true
	s.lsnCurrent = slotLSN
	events := make(chan *CDCEvent, 4096)

	go s.streamLoop(ctx, conn, events)

	s.log.Info("cdc replication started",
		zap.String("slot", s.cfg.SlotName),
		zap.String("lsn", slotLSN.String()),
	)
	return events, nil
}

// streamLoop is the main WAL consumer loop.
func (s *Source) streamLoop(ctx context.Context, conn *pgconn.PgConn, events chan<- *CDCEvent) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		close(events)
		conn.Close(ctx)
	}()

	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	// Track relations learned from relation messages
	relations := make(map[uint32]*Relation)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("cdc stream context cancelled")
			return
		case <-s.stopCh:
			s.log.Info("cdc stream stopped")
			return
		default:
		}

		// Receive next message with deadline
		rawMsg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("cdc receive message error", zap.Error(err))
			// Try to reconnect? For now, exit
			return
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			s.log.Error("cdc error response",
				zap.String("severity", errMsg.Severity),
				zap.String("message", errMsg.Message),
			)
			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		// Parse the copy data
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				s.log.Error("parse keepalive", zap.Error(err))
				continue
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{} // force immediate reply
			}
			if pkm.ServerWALEnd > s.lsnCurrent {
				s.lsnCurrent = pkm.ServerWALEnd
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				s.log.Error("parse xlogdata", zap.Error(err))
				continue
			}
			if xld.WALStart > s.lsnCurrent {
				s.lsnCurrent = xld.WALStart
			}

			// Parse the logical replication message
			event := s.parseLogicalMsg(relations, xld)
			if event != nil {
				select {
				case events <- event:
					s.eventsReceived++
				case <-ctx.Done():
					return
				case <-s.stopCh:
					return
				}
			}
		}

		// Send standby status update periodically
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: s.lsnCurrent,
				})
			if err != nil {
				s.log.Error("send standby status", zap.Error(err))
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}
	}
}

// parseLogicalMsg converts a pgoutput logical replication message into a CDCEvent.
func (s *Source) parseLogicalMsg(relations map[uint32]*Relation, xld pglogrepl.XLogData) *CDCEvent {
	// inStream=false: pglogrepl.ParseV2's second arg is the streaming-large-tx flag.
	// It must be true ONLY while inside a StreamStart..StreamStop sequence (the WAL
	// records then carry a 4-byte XID prefix). This client does not implement the
	// streaming protocol, so every message is a normal committed-transaction record
	// with NO XID prefix → inStream must be false. Passing true made readXidAndAdvance
	// consume 4 phantom bytes, misaligning every Insert/Update/Delete decode
	// (expect N/K/O, actual \x00) and silently dropping all DML. See #t48 Bug#4.
	logMsg, err := pglogrepl.ParseV2(xld.WALData, false)
	if err != nil {
		s.log.Error("parse logical msg", zap.Error(err))
		return nil
	}

	switch v := logMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		// Learn the relation schema
		rel := &Relation{
			OID:    v.RelationID,
			Schema: v.Namespace,
			Name:   v.RelationName,
		}
		for i, col := range v.Columns {
			rel.Columns = append(rel.Columns, RelationColumn{
				Name:    col.Name,
				TypeOID: col.DataType,
				TypeName: fmt.Sprintf("oid_%d", col.DataType),
				IsKey:   col.Flags == 1,
				Ordinal: i,
			})
		}
		relations[v.RelationID] = rel
		return nil // relation message is not a data event

	case *pglogrepl.InsertMessageV2:
		rel, ok := relations[v.RelationID]
		if !ok {
			return nil
		}
		return &CDCEvent{
			LSN:       xld.WALStart,
			Timestamp: time.Now(),
			Kind:      EventInsert,
			Schema:    rel.Schema,
			Table:     rel.Name,
			Columns:   decodeTupleColumns(rel, v.Tuple, false), // full new image ('N')
		}

	case *pglogrepl.UpdateMessageV2:
		rel, ok := relations[v.RelationID]
		if !ok {
			return nil
		}
		// Under REPLICA IDENTITY DEFAULT a non-key UPDATE carries NO old tuple;
		// the transformer then builds WHERE from the new image's PK columns
		// (PK is unchanged). When an old tuple IS present, 'K' carries only the
		// PK columns while 'O' (FULL) carries all columns — map accordingly.
		var oldCols []ColumnValue
		if v.OldTuple != nil {
			oldCols = decodeTupleColumns(rel, v.OldTuple,
				v.OldTupleType == pglogrepl.UpdateMessageTupleTypeKey)
		}
		return &CDCEvent{
			LSN:        xld.WALStart,
			Timestamp:  time.Now(),
			Kind:       EventUpdate,
			Schema:     rel.Schema,
			Table:      rel.Name,
			Columns:    decodeTupleColumns(rel, v.NewTuple, false), // full new image ('N')
			OldColumns: oldCols,
		}

	case *pglogrepl.DeleteMessageV2:
		rel, ok := relations[v.RelationID]
		if !ok {
			return nil
		}
		// DELETE always carries a key/old tuple: 'K' (PK only) under DEFAULT,
		// 'O' (all columns) under FULL. Map accordingly so the PK columns keep
		// correct names + IsKey regardless of replica-identity mode.
		var cols []ColumnValue
		if v.OldTuple != nil {
			cols = decodeTupleColumns(rel, v.OldTuple,
				v.OldTupleType == pglogrepl.DeleteMessageTupleTypeKey)
		}
		return &CDCEvent{
			LSN:       xld.WALStart,
			Timestamp: time.Now(),
			Kind:      EventDelete,
			Schema:    rel.Schema,
			Table:     rel.Name,
			Columns:   cols,
		}

	case *pglogrepl.TruncateMessageV2:
		if len(v.RelationIDs) == 0 {
			return nil
		}
		rel, ok := relations[v.RelationIDs[0]]
		if !ok {
			return nil
		}
		return &CDCEvent{
			LSN:       xld.WALStart,
			Timestamp: time.Now(),
			Kind:      EventTruncate,
			Schema:    rel.Schema,
			Table:     rel.Name,
		}

	default:
		return nil
	}
}

// decodeTupleColumns maps a pgoutput TupleData onto ColumnValues using the
// relation schema learned from RelationMessageV2.
//
// pgoutput tuple images differ in width depending on replica identity:
//   - 'N' (new image) and 'O' (FULL old image) carry ALL relation columns,
//     positionally aligned with rel.Columns.
//   - 'K' (key image, used by DEFAULT replica identity for UPDATE-of-PK and all
//     DELETE) carries ONLY the PK columns, in relation column order.
//
// isKeyTuple selects between the two mappings. NULL columns ('n') map to a nil
// value so the transformer renders NULL (not ''). Every column carries the
// relation's IsKey flag so the transformer can build a PK-only WHERE without
// relying on old/new image presence. See #t48 Bug#5.
func decodeTupleColumns(rel *Relation, tuple *pglogrepl.TupleData, isKeyTuple bool) []ColumnValue {
	if tuple == nil {
		return nil
	}
	out := make([]ColumnValue, 0, len(tuple.Columns))
	if isKeyTuple {
		// Key image: only PK columns, in relation order.
		ki := 0
		for _, rc := range rel.Columns {
			if !rc.IsKey {
				continue
			}
			if ki >= len(tuple.Columns) {
				break
			}
			out = append(out, decodeColumnValue(rc, tuple.Columns[ki]))
			ki++
		}
		return out
	}
	// Full image: positional alignment with rel.Columns.
	for i, c := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		out = append(out, decodeColumnValue(rel.Columns[i], c))
	}
	return out
}

// decodeColumnValue builds a ColumnValue from a single tuple column, carrying
// the relation column's name/type/IsKey and rendering NULL ('n') as nil.
func decodeColumnValue(rc RelationColumn, c *pglogrepl.TupleDataColumn) ColumnValue {
	cv := ColumnValue{
		Name:  rc.Name,
		Type:  rc.TypeName,
		IsKey: rc.IsKey,
	}
	if c != nil && c.DataType == pglogrepl.TupleDataTypeNull {
		cv.Value = nil
	} else {
		// 't' text / 'b' binary -> text representation; 'u' unchanged -> no value.
		cv.Value = string(c.Data)
	}
	return cv
}

// Stop gracefully stops the replication stream.
func (s *Source) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		close(s.stopCh)
	}
}

// CurrentLSN returns the most recently observed LSN.
func (s *Source) CurrentLSN() pglogrepl.LSN {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lsnCurrent
}

// EventsReceived returns the count of events received.
func (s *Source) EventsReceived() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eventsReceived
}

// IsRunning returns whether the source is actively streaming.
func (s *Source) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// quoteIdent double-quotes a PostgreSQL identifier.
func quoteIdent(s string) string {
	return `"` + s + `"`
}
