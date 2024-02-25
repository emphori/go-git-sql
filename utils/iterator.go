package utils

import (
	"database/sql"
	"io"
)

type Iterator[T interface{}] struct {
	rows *sql.Rows
	scan func(*sql.Rows) (T, error)
}

func NewIterator[T interface{}](rows *sql.Rows, scan func(*sql.Rows) (T, error)) (*Iterator[T], error) {
	return &Iterator[T]{
		rows: rows,
		scan: scan,
	}, nil
}

func (i *Iterator[T]) Next() (T, error) {
	if i.rows.Next() {
		return i.scan(i.rows)
	} else {
		i.Close()
		var result T
		return result, io.EOF
	}
}

func (i *Iterator[T]) ForEach(fn func(T) error) error {
	for i.rows.Next() {
		row, err := i.scan(i.rows)

		if err != nil {
			return nil
		}

		fn(row)
	}

	i.Close()
	return io.EOF
}

func (i *Iterator[T]) Close() {
	i.rows.Close()
}
