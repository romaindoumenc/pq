package pq

import (
	"bytes"
	"database/sql"
)

/*
begin;
select lo_open(20483, 262144);
select loread(0, 256);
commit;
*/

const (
	loreadonly = 0x40000
)

// LargeObject: wrapper around the postgresql large object structure,
// providing a convenient io.Reader interface
type LargeObject struct {
	fh int // handle to the oid
	bf *bytes.Buffer // internal buffer
	tx *sql.Tx // db transaction with handler validity
}

func OpenLO(db *sql.DB, oid int) (*LargeObject, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	
	rw := tx.QueryRow("select lo_open($1, $2);", oid, loreadonly)
	var fh int
	err = rw.Scan(&fh)
	if err != nil {
		return nil, err
	}

	return &LargeObject{
		fh: fh,
		tx: tx,
		bf: new(bytes.Buffer),
	}, nil
}

func (lo *LargeObject) Read(p []byte) (int, error) {
	if len(p) > lo.bf.Len() {
		bf := make([]byte, 512)
		rw := lo.tx.QueryRow("select loread($1, 512);", lo.fh)
		err := rw.Scan(&bf)
		if err != nil {
			return 0, err
		}
		lo.bf.Write(bf)
	}
	return lo.bf.Read(p)
}

func (lo *LargeObject) Close() error {
	lo.tx.Query("select lo_close($1);", lo.fh)
	return lo.tx.Commit()
}
