package validator

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"go.uber.org/zap"
)

// validateChecksumChunked computes row hashes for each chunk of the table
// in parallel, then compares PG and TiDB chunk by chunk.
func (v *Validator) validateChecksumChunked(ctx context.Context, pgDB, tidbDB *sql.DB, table string) reporter.TableReport {
	tr := reporter.TableReport{TableName: table, Status: reporter.StatusPass}

	// First do exact row count
	tr = v.validateRowCount(ctx, pgDB, tidbDB, table)
	if tr.Status == reporter.StatusFail && tr.DiffRows != 0 {
		return tr
	}

	if tr.SourceRows == 0 {
		tr.Status = reporter.StatusPass
		return tr
	}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	// Detect key columns for chunking
	keyInfo, _ := v.detectTableKey(ctx, pgDB, schema, table)
	var orderByCols string
	if keyInfo != nil && keyInfo.HasPK {
		orderByCols = strings.Join(keyInfo.PKColumns, ", ")
	} else if keyInfo != nil && keyInfo.HasUniqueIndex {
		orderByCols = strings.Join(keyInfo.UniqueColumns, ", ")
	} else {
		// No key — fall back to hash_group comparison (already implemented)
		logger := zap.L()
		logger.Info("checksum mode: no PK/unique for chunking, falling back to hash_group", zap.String("table", table))
		return v.validateSamplingWithHashGroup(ctx, pgDB, tidbDB, table, 1.0, tr, schema)
	}

	chunkSize := v.cfg.Compare.ChecksumChunkSize
	if chunkSize <= 0 {
		chunkSize = 50000
	}
	parallel := v.cfg.Compare.ChecksumParallel
	if parallel <= 0 {
		parallel = 4
	}

	totalRows := tr.SourceRows
	numChunks := int(totalRows / chunkSize)
	if totalRows%chunkSize > 0 {
		numChunks++
	}
	if numChunks == 0 {
		numChunks = 1
	}

	logger := zap.L()
	logger.Info("checksum mode: chunked parallel hash",
		zap.String("table", table),
		zap.Int64("total_rows", totalRows),
		zap.Int("chunks", numChunks),
		zap.Int64("chunk_size", chunkSize))

	// Build chunk boundaries using the key column
	chunks := make([]chunkRange, numChunks)
	for i := 0; i < numChunks; i++ {
		chunks[i] = chunkRange{
			offset: int64(i) * chunkSize,
			limit:  chunkSize,
		}
		// Adjust last chunk
		if i == numChunks-1 {
			chunks[i].limit = totalRows - chunks[i].offset
		}
	}

	// Process chunks in parallel
	var mu sync.Mutex
	var mismatchDetails []string
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup

	for i, chunk := range chunks {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, ch chunkRange) {
			defer wg.Done()
			defer func() { <-sem }()

			pgHash, err := v.computeChunkHashPG(ctx, pgDB, schema, table, orderByCols, ch)
			if err != nil {
				mu.Lock()
				mismatchDetails = append(mismatchDetails, fmt.Sprintf("chunk %d: PG error: %v", idx, err))
				mu.Unlock()
				return
			}

			tidbHash, err := v.computeChunkHashTiDB(ctx, tidbDB, table, orderByCols, ch)
			if err != nil {
				mu.Lock()
				mismatchDetails = append(mismatchDetails, fmt.Sprintf("chunk %d: TiDB error: %v", idx, err))
				mu.Unlock()
				return
			}

			if pgHash != tidbHash {
				mu.Lock()
				mismatchDetails = append(mismatchDetails, fmt.Sprintf("chunk %d (rows %d-%d): hash mismatch pg=%s tidb=%s",
					idx, ch.offset, ch.offset+ch.limit, truncate(pgHash, 12), truncate(tidbHash, 12)))
				mu.Unlock()
			}
		}(i, chunk)
	}
	wg.Wait()

	if len(mismatchDetails) > 0 {
		tr.Status = reporter.StatusFail
		maxShow := 10
		if len(mismatchDetails) > maxShow {
			mismatchDetails = mismatchDetails[:maxShow]
		}
		tr.Error = fmt.Sprintf("checksum mismatch in %d/%d chunks: %s",
			len(mismatchDetails), numChunks, strings.Join(mismatchDetails, "; "))
	} else {
		tr.Status = reporter.StatusPass
	}

	tr.Suggestion = fmt.Sprintf("checksum mode: %d chunks × %d rows, %d mismatches",
		numChunks, chunkSize, len(mismatchDetails))

	return tr
}

type chunkRange struct {
	offset int64
	limit  int64
}

// computeChunkHashPG computes an aggregate hash for a chunk of PG rows.
func (v *Validator) computeChunkHashPG(ctx context.Context, pgDB *sql.DB, schema, table, orderBy string, ch chunkRange) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d",
		quotePG(schema), quotePG(table), orderBy, ch.limit, ch.offset)

	rows, err := pgDB.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, _ := rows.ColumnTypes()
	if cols == nil {
		return "", fmt.Errorf("no column types")
	}

	// Build skip cols and sorted column list
	skipCols := make(map[int]bool)
	colNames := make([]string, len(cols))
	colIdxMap := make(map[string]int)
	for i, c := range cols {
		colNames[i] = c.Name()
		colIdxMap[strings.ToLower(c.Name())] = i
		dt := strings.ToLower(c.DatabaseTypeName())
		if isApproximateFloatType(dt) || strings.Contains(dt, "json") {
			skipCols[i] = true
		}
	}

	sortedCols := make([]string, len(colNames))
	copy(sortedCols, colNames)
	sort.Strings(sortedCols)

	// Build ordered non-skipped column indices
	var hashIdxs []int
	for _, name := range sortedCols {
		idx := colIdxMap[strings.ToLower(name)]
		if !skipCols[idx] {
			hashIdxs = append(hashIdxs, idx)
		}
	}

	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	var rowHashes []string
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		var buf strings.Builder
		for i, idx := range hashIdxs {
			if i > 0 {
				buf.WriteByte('|')
			}
			val := normalizeValue(values[idx])
			buf.WriteString(val)
		}
		rowHashes = append(rowHashes, fmt.Sprintf("%x", md5.Sum([]byte(buf.String()))))
	}

	sort.Strings(rowHashes)
	return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(rowHashes, ",")))), nil
}

// computeChunkHashTiDB computes an aggregate hash for a chunk of TiDB rows.
func (v *Validator) computeChunkHashTiDB(ctx context.Context, tidbDB *sql.DB, table, orderBy string, ch chunkRange) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d OFFSET %d",
		quoteMySQL(table), quoteMySQL(orderBy), ch.limit, ch.offset)

	rows, err := tidbDB.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, _ := rows.ColumnTypes()
	if cols == nil {
		return "", fmt.Errorf("no column types")
	}

	skipCols := make(map[int]bool)
	colNames := make([]string, len(cols))
	colIdxMap := make(map[string]int)
	for i, c := range cols {
		colNames[i] = c.Name()
		colIdxMap[strings.ToLower(c.Name())] = i
		dt := strings.ToLower(c.DatabaseTypeName())
		if isApproximateFloatType(dt) || strings.Contains(dt, "json") {
			skipCols[i] = true
		}
	}

	sortedCols := make([]string, len(colNames))
	copy(sortedCols, colNames)
	sort.Strings(sortedCols)

	var hashIdxs []int
	for _, name := range sortedCols {
		idx := colIdxMap[strings.ToLower(name)]
		if !skipCols[idx] {
			hashIdxs = append(hashIdxs, idx)
		}
	}

	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	var rowHashes []string
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		var buf strings.Builder
		for i, idx := range hashIdxs {
			if i > 0 {
				buf.WriteByte('|')
			}
			val := normalizeValue(values[idx])
			buf.WriteString(val)
		}
		rowHashes = append(rowHashes, fmt.Sprintf("%x", md5.Sum([]byte(buf.String()))))
	}

	sort.Strings(rowHashes)
	return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(rowHashes, ",")))), nil
}
