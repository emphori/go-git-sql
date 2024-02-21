# go-git-sql

A barebones SQL-backed storage implementation for [go-git][go-git]

[go-git]: https://github.com/go-git/go-git

## Creating SQL tables

```sql
CREATE TABLE IF NOT EXISTS "objects" (
  object_type BIGINT,
  object_hash VARCHAR,
  object_size BIGINT
  cont BYTEA,
);

CREATE TABLE IF NOT EXISTS "refs" (
  ref_type BIGINT,
  ref_hash VARCHAR,
  ref_name VARCHAR,
  target VARCHAR
);
```