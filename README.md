# go-git-sql

An SQL-backed storage implementation for [go-git][go-git].

[go-git]: https://github.com/go-git/go-git

> [!WARNING]
>
> This project is currently a work in progress, and may not include the
> functionality you expect. Please refer to [this milestone] for more details.
>
> [this milestone]: https://github.com/emphori/go-git-sql/milestone/1

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
