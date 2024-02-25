package storage

import (
	"database/sql"
	"fmt"

	"github.com/emphori/go-git-sql/utils"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

type ObjectStorage struct {
	client *sql.DB
}

// NewEncodedObject returns a new copy of the default plumbing.MemoryObject.
func (o *ObjectStorage) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{}
}

// EncodedObject returns the object with the specified hash and matching object
// type. If AnyObject is passed as the object type, only the object hash will be
// used in this lookup.
func (o *ObjectStorage) EncodedObject(objType plumbing.ObjectType, objHash plumbing.Hash) (plumbing.EncodedObject, error) {
	rows, err := o.client.Query(`SELECT type, cont FROM "objects" WHERE hash = $1;`, objHash.String())

	if err != nil {
		return nil, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	defer rows.Close()

	if !rows.Next() {
		return nil, plumbing.ErrObjectNotFound
	}

	obj, err := scanMemoryObject(rows)
	if err != nil {
		return nil, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	// If the object found in the database does not have the correct object type,
	// return an error. If the expected object type is AnyObject, skip this check.
	if objType != plumbing.AnyObject && obj.Type() != objType {
		return nil, plumbing.ErrObjectNotFound
	}

	return obj, nil
}

// IterEncodedObjects returns an iterator that traverses all of the available
// objects of the specified type.
func (o *ObjectStorage) IterEncodedObjects(objType plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	rows, err := o.client.Query(`SELECT type, cont FROM "objects" WHERE type = $1;`, objType)

	if err != nil {
		return nil, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	return utils.NewIterator(rows, scanMemoryObject)
}

// EncodedObjectSize returns the size of the contents stored against the object
// with the specified hash.
func (o *ObjectStorage) EncodedObjectSize(hash plumbing.Hash) (int64, error) {
	rows, err := o.client.Query(`SELECT length FROM "objects" WHERE hash = $1;`, hash)

	if err != nil {
		return 0, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	defer rows.Close()

	var len int64
	err = rows.Scan(&len)
	if err != nil {
		return 0, plumbing.ErrObjectNotFound
	}
	return len, nil
}

// HasEncodedObject checks if an object with the specified hash exists,
// returning nil if the object does exist, and an error if it does not.
func (o *ObjectStorage) HasEncodedObject(hash plumbing.Hash) error {
	rows, err := o.client.Query(`SELECT hash FROM "objects" WHERE hash = '$1';`, hash)

	if err != nil {
		return 0, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	defer rows.Close()

	if _, err = scanMemoryObject(rows); err != nil {
		return plumbing.ErrObjectNotFound
	}

	return nil
}

// SetEncodedObject stores the object provided.
//
// If the object contents cannot be read, or the object fails to be written to
// the database, a ZeroHash will be returned with the appropriate wrapped error.
func (o *ObjectStorage) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	cont := make([]byte, obj.Size())
	reader, _ := obj.Reader()

	// Read the provided objects contents in to a local byte array.
	//
	// In the event that this operation fails, return a ZeroHash along with the
	// error returned by the Reader.
	if _, err := reader.Read(cont); err != nil {
		return plumbing.ZeroHash, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	// Write the object to the database.
	//
	// Similar to the above, if an error occurs we will return a ZeroHash along
	// with the error returned by the database driver.
	if _, err := o.client.Exec(`INSERT INTO objects(type, hash, cont, length) VALUES($1, $2, $3, $4);`, obj.Type(), obj.Hash(), cont, obj.Size()); err != nil {
		return plumbing.ZeroHash, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	return obj.Hash(), nil
}

// AddAlternate is not currently implemented.
func (o *ObjectStorage) AddAlternate(remote string) error {
	return &plumbing.UnexpectedError{
		Err: fmt.Errorf("Not supported"),
	}
}

func scanMemoryObject(row *sql.Rows) (plumbing.EncodedObject, error) {
	var t plumbing.ObjectType
	var cont []byte

	// Attempt to scan the row, and in the event that an error occurs, it's likely
	// we have hit a dud query. If this happens, return nil and the wrapped error.
	if err := row.Scan(&t, &cont); err != nil {
		return nil, &plumbing.UnexpectedError{
			Err: err,
		}
	}

	obj := &plumbing.MemoryObject{}

	obj.SetType(t)
	obj.Write(cont)

	return obj, nil
}
