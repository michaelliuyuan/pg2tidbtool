package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
)

type TargetChecker struct {
	cfg        config.Config
	existTables map[string]bool
}

func NewTargetChecker(cfg config.Config) *TargetChecker {
	return &TargetChecker{cfg: cfg, existTables: make(map[string]bool)}
}

func (tc *TargetChecker) LoadExistingTables(ctx context.Context) error {
	db, err := sql.Open("mysql", tc.cfg.Target.DSN())
	if err != nil {
		return fmt.Errorf("connect to TiDB for table check: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", tc.cfg.Target.Database)
	if err != nil {
		return fmt.Errorf("query existing tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		tc.existTables[strings.ToLower(name)] = true
	}
	return nil
}

func (tc *TargetChecker) TableExists(name string) bool {
	return tc.existTables[strings.ToLower(name)]
}

func (tc *TargetChecker) ExistingTables() map[string]bool {
	return tc.existTables
}
