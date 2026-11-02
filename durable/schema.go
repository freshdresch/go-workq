package durable

import (
	"context"
	"fmt"
)

const schemaDDL = `
CREATE TABLE meta (
 key INTEGER NOT NULL PRIMARY KEY CHECK(typeof(key)='integer' AND key BETWEEN 1 AND 15),
 value BLOB NOT NULL CHECK(typeof(value)='blob'),
 CHECK(length(value)=CASE key
  WHEN 2 THEN 13 WHEN 5 THEN 16 WHEN 6 THEN 56 WHEN 7 THEN 1 ELSE 8 END)
) WITHOUT ROWID;
CREATE TABLE producers (
 producer BLOB NOT NULL PRIMARY KEY CHECK(typeof(producer)='blob' AND length(producer)=16),
 has_retirement INTEGER NOT NULL
  CHECK(typeof(has_retirement)='integer' AND has_retirement IN (0,1)),
 retired_through BLOB NOT NULL
  CHECK(typeof(retired_through)='blob' AND length(retired_through)=8),
 cleanup_needed INTEGER NOT NULL
  CHECK(typeof(cleanup_needed)='integer' AND cleanup_needed IN (0,1))
) WITHOUT ROWID;
CREATE TABLE receipts (
 id BLOB NOT NULL PRIMARY KEY CHECK(typeof(id)='blob' AND length(id)=24),
 digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32),
 state INTEGER NOT NULL CHECK(typeof(state)='integer' AND state IN (0,1)),
 accept_order BLOB NOT NULL UNIQUE CHECK(typeof(accept_order)='blob' AND length(accept_order)=8),
 ack_token BLOB,
 CHECK((state=0 AND ack_token IS NULL) OR
  (state=1 AND typeof(ack_token)='blob' AND length(ack_token)=56))
) WITHOUT ROWID;
CREATE TABLE payloads (
 id BLOB NOT NULL PRIMARY KEY CHECK(typeof(id)='blob' AND length(id)=24),
 codec BLOB NOT NULL CHECK(typeof(codec)='blob' AND length(codec)<=64),
 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=16384),
 payload_length INTEGER NOT NULL
  CHECK(typeof(payload_length)='integer' AND payload_length=length(payload))
) WITHOUT ROWID;
CREATE INDEX pending_order ON receipts(accept_order,id) WHERE state=0;
CREATE INDEX pending_producer ON receipts(id) WHERE state=0;
CREATE INDEX cleanup_producers ON producers(producer) WHERE cleanup_needed=1;
`

func (s *store) createSchema(ctx context.Context) error {
	if _, err := s.conn.ExecContext(ctx, schemaDDL); err != nil {
		return fmt.Errorf("create schema: %w: %w", ErrStorage, err)
	}
	return nil
}
