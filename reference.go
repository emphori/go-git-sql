package storage

import (
	"database/sql"
	"fmt"

	"github.com/emphori/go-git-sql/utils"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"
)

type ReferenceStorage struct {
	client *sql.DB
}

// Reference loads a Git reference from storage.
func (r *ReferenceStorage) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	rows, err := r.client.Query(`SELECT type, hash, name, target FROM "refs" WHERE name = $1;`, name)
	if err != nil {
		return nil, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	if !rows.Next() {
		return nil, plumbing.ErrReferenceNotFound
	}

	obj, err := scanReference(rows)
	if err != nil {
		return nil, plumbing.ErrReferenceNotFound
	}

	rows.Close()
	return obj, nil
}

// IterReferences returns an iterator capable of walking through all available
// Git references.
func (r *ReferenceStorage) IterReferences() (storer.ReferenceIter, error) {
	rows, err := r.client.Query(`SELECT type, hash, name, target FROM "refs";`)

	if err != nil {
		return nil, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	return utils.NewIterator(rows, scanReference)
}

// SetReference writes a Git reference to storage.
func (r *ReferenceStorage) SetReference(ref *plumbing.Reference) error {
	if _, err := r.client.Exec(`INSERT INTO refs(type, hash, name, target) VALUES($1, $2, $3, $4);`, ref.Type(), ref.Hash(), ref.Name(), ref.Target()); err != nil {
		return &plumbing.UnexpectedError{
			Err: err,
		}
	}

	return nil
}

// RemoveReference deletes a Git reference from storage by its unique name.
func (r *ReferenceStorage) RemoveReference(name plumbing.ReferenceName) error {
	if _, err := r.client.Exec(`DELETE FROM "refs" WHERE name = $1;`, name); err != nil {
		return &plumbing.UnexpectedError{
			Err: err,
		}
	}

	return nil
}

func (r *ReferenceStorage) CheckAndSetReference(new, old *plumbing.Reference) error {
	if new == nil {
		return nil
	}

	if old != nil {
		if tmp, _ := r.Reference(old.Name()); tmp != nil && tmp.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
	}

	tmp, _ := r.Reference(new.Name())
	if tmp != nil {
		if err := r.RemoveReference(new.Name()); err != nil {
			return &plumbing.UnexpectedError{
				Err: err,
			}
		}
	}

	return r.SetReference(new)

}

func (r *ReferenceStorage) CountLooseRefs() (int, error) {
	query, err := r.client.Query(`SELECT COUNT(*) FROM "refs";`)

	if err != nil {
		return 0, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	defer query.Close()

	var count int
	query.Scan(&count)
	return count, nil
}

// PackRefs is not currently implemented.
func (r *ReferenceStorage) PackRefs() error {
	return &plumbing.UnexpectedError{
		Err: fmt.Errorf("Not supported"),
	}
}

func scanReference(row *sql.Rows) (*plumbing.Reference, error) {
	var t plumbing.ReferenceType

	var hash string
	var name plumbing.ReferenceName
	var target plumbing.ReferenceName

	if err := row.Scan(&t, &hash, &name, &target); err != nil {
		return nil, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	switch t {
	case plumbing.HashReference:
		return plumbing.NewHashReference(name, plumbing.NewHash(hash)), nil

	case plumbing.SymbolicReference:
		return plumbing.NewSymbolicReference(name, target), nil

	default:
		return nil, &plumbing.UnexpectedError{
			Err: fmt.Errorf("unhandled ref type: %s", t.String()),
		}
	}
}
