package durable

import (
	"context"
	"errors"
)

const migrationKeysQuery = `SELECT id FROM migration_payloads
 WHERE id>? ORDER BY id LIMIT 256`

const migrationCopyQuery = `INSERT INTO payloads(id,codec,payload,payload_length)
 SELECT id,codec,payload,payload_length FROM migration_payloads WHERE id=?`

func (s *validatedStore) migratePayloads(ctx context.Context, observer offlineObserver) error {
	for _, step := range []struct {
		query string
		stage string
	}{
		{"ALTER TABLE payloads RENAME TO migration_payloads", "migration-renamed"},
		{payloadDDLV2, "migration-created"},
	} {
		if _, err := s.storage.conn.ExecContext(ctx, step.query); err != nil {
			return classifySQLError(err)
		}
		if err := observeOffline(observer, step.stage, s); err != nil {
			return err
		}
	}
	// The empty BLOB precedes every valid 24-byte ID, including the all-zero ID.
	lower := []byte{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		keys, count, err := s.storage.migrationKeys(ctx, lower)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		for i := range count {
			if _, err := s.storage.conn.ExecContext(ctx, migrationCopyQuery, keys[i][:]); err != nil {
				return classifySQLError(err)
			}
		}
		if err := observeOffline(observer, "migration-batch", s); err != nil {
			return err
		}
		lower = keys[count-1][:]
	}
	if _, err := s.storage.conn.ExecContext(ctx, "DROP TABLE migration_payloads"); err != nil {
		return classifySQLError(err)
	}
	if err := observeOffline(observer, "migration-dropped", s); err != nil {
		return err
	}
	version, err := schemaVersionForProfile(profileSQLiteWALV2)
	if err != nil {
		return err
	}
	encoded := encodeUint64(version)
	for _, field := range []struct {
		key   int
		value []byte
	}{
		{metaSchema, encoded[:]},
		{metaProfile, []byte(profileSQLiteWALV2)},
	} {
		if _, err := s.storage.conn.ExecContext(
			ctx,
			"UPDATE meta SET value=? WHERE key=?",
			field.value,
			field.key,
		); err != nil {
			return classifySQLError(err)
		}
	}
	return observeOffline(observer, "migration-metadata", s)
}

func (s *store) migrationKeys(
	ctx context.Context,
	lower []byte,
) (keys [256][24]byte, count int, err error) {
	rows, err := s.conn.QueryContext(ctx, migrationKeysQuery, lower)
	if err != nil {
		return keys, 0, classifySQLError(err)
	}
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			return keys, 0, classifySQLError(errors.Join(err, rows.Close()))
		}
		if len(key) != 24 || count == len(keys) {
			return keys, 0, errors.Join(ErrStorage, rows.Close())
		}
		copy(keys[count][:], key)
		count++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return keys, 0, classifySQLError(err)
	}
	return keys, count, nil
}
