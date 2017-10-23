package processors

import (
	"github.com/jmoiron/sqlx"

	"github.com/fefelovgroup/ratchet/data"
)

// SQLReaderSQLiteWriter performs both the job of a SQLReader and SQLiteWriter.
// This means it will run a SQL query, write the resulting data into a
// MySQL database, and (if the write was successful) send the queried data
// to the next stage of processing.
//
// SQLReaderSQLiteWriter is composed of both a SQLReader and SQLiteWriter, so it
// supports all of the same properties and usage options (such as static
// versus dynamic SQL querying).
type SQLReaderSQLiteWriter struct {
	SQLReader
	SQLiteWriter
	ConcurrencyLevel int // See ConcurrentDataProcessor
}

// NewSQLReaderSQLiteWriter returns a new SQLReaderSQLiteWriter ready for static querying.
func NewSQLReaderSQLiteWriter(readConn *sqlx.DB, writeConn *sqlx.DB, readQuery, writeTable string) *SQLReaderSQLiteWriter {
	s := SQLReaderSQLiteWriter{}
	s.SQLReader = *NewSQLReader(readConn, readQuery)
	s.SQLiteWriter = *NewSQLiteWriter(writeConn, writeTable)
	return &s
}

// NewDynamicSQLReaderSQLiteWriter returns a new SQLReaderSQLiteWriter ready for dynamic querying.
func NewDynamicSQLReaderSQLiteWriter(readConn *sqlx.DB, writeConn *sqlx.DB, sqlGenerator func(data.JSON) (string, error), writeTable string) *SQLReaderSQLiteWriter {
	s := NewSQLReaderSQLiteWriter(readConn, writeConn, "", writeTable)
	s.sqlGenerator = sqlGenerator
	return s
}

// ProcessData uses SQLReader methods for processing data - this works via composition
func (s *SQLReaderSQLiteWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	s.ForEachQueryData(d, killChan, func(d data.JSON) {
		s.SQLiteWriter.ProcessData(d, outputChan, killChan)
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (s *SQLReaderSQLiteWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLReaderSQLiteWriter) String() string {
	return "SQLReaderSQLiteWriter"
}

// Concurrency defers to ConcurrentDataProcessor
func (s *SQLReaderSQLiteWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
