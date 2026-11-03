package inventory

import (
	"context"
	"errors"
	"fmt"
)

const metadataSchema = `CREATE TABLE metadata (
 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
 version INTEGER NOT NULL CHECK(version = 1),
 receipt_limit INTEGER NOT NULL CHECK(receipt_limit > 0)
)`

const inventorySchema = `CREATE TABLE inventory (
 item TEXT PRIMARY KEY NOT NULL CHECK(item = 'widget'),
 quantity INTEGER NOT NULL CHECK(typeof(quantity) = 'integer' AND quantity >= 0),
 adjustments INTEGER NOT NULL CHECK(typeof(adjustments) = 'integer' AND adjustments >= 0)
)`

const receiptsSchema = `CREATE TABLE receipts (
 version INTEGER NOT NULL CHECK(version = 1),
 namespace TEXT NOT NULL CHECK(length(CAST(namespace AS BLOB)) BETWEEN 1 AND 64),
 producer BLOB NOT NULL CHECK(typeof(producer) = 'blob' AND length(producer) = 16),
 sequence BLOB NOT NULL CHECK(typeof(sequence) = 'blob' AND length(sequence) = 8),
 effect TEXT NOT NULL CHECK(length(CAST(effect AS BLOB)) BETWEEN 1 AND 64),
 codec TEXT NOT NULL CHECK(length(CAST(codec AS BLOB)) BETWEEN 1 AND 64),
 request BLOB NOT NULL CHECK(typeof(request) = 'blob' AND length(request) <= 1024),
 result_quantity INTEGER CHECK(result_quantity IS NULL OR
  (typeof(result_quantity) = 'integer' AND result_quantity >= 0)),
 PRIMARY KEY(version, namespace, producer, sequence, effect)
)`

const schema = metadataSchema + ";\n" + inventorySchema + ";\n" + receiptsSchema + ";"

func (s *Store) configureAndInitialize(ctx context.Context, created bool) error {
	var journal string
	if err := s.conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return err
	}
	if journal != "wal" {
		return fmt.Errorf("inventory journal mode is %q, want wal", journal)
	}
	for _, setting := range []struct {
		name string
		want int
	}{
		{"synchronous", 2}, {"busy_timeout", 250}, {"foreign_keys", 1},
	} {
		var got int
		if err := s.conn.QueryRowContext(ctx, "PRAGMA "+setting.name).Scan(&got); err != nil {
			return err
		}
		if got != setting.want {
			return fmt.Errorf("inventory %s is %d", setting.name, got)
		}
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	if created {
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			return s.rollback(tx, err)
		}
		_, err = tx.ExecContext(ctx,
			"INSERT INTO metadata VALUES (1, 1, ?)", s.receiptLimit)
		if err != nil {
			return s.rollback(tx, err)
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO inventory VALUES ('widget', 10, 0)")
		if err != nil {
			return s.rollback(tx, err)
		}
	}
	// Exact definitions preserve the identity collation and uniqueness contract.
	// Additional test tables and triggers do not change these three definitions.
	for _, table := range []struct {
		name       string
		definition string
	}{
		{"metadata", metadataSchema},
		{"inventory", inventorySchema},
		{"receipts", receiptsSchema},
	} {
		var definition string
		err := tx.QueryRowContext(
			ctx,
			"SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
			table.name,
		).Scan(&definition)
		if err != nil {
			return s.rollback(tx, fmt.Errorf("inventory table %q: %w", table.name, err))
		}
		if definition != table.definition {
			return s.rollback(tx, fmt.Errorf("unexpected inventory table %q definition", table.name))
		}
	}
	var version, limit int
	err = tx.QueryRowContext(ctx,
		"SELECT version, receipt_limit FROM metadata WHERE singleton = 1").Scan(&version, &limit)
	if err != nil {
		return s.rollback(tx, fmt.Errorf("inventory metadata: %w", err))
	}
	if version != 1 || limit != s.receiptLimit {
		return s.rollback(tx, errors.New("inventory schema version or receipt limit mismatch"))
	}
	var incomplete int
	err = tx.QueryRowContext(ctx,
		"SELECT count(*) FROM receipts WHERE result_quantity IS NULL").Scan(&incomplete)
	if err != nil {
		return s.rollback(tx, err)
	}
	if incomplete != 0 {
		return s.rollback(tx, errors.New("committed incomplete inventory receipt"))
	}
	return s.commit(ctx, tx)
}
