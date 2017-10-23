package processors

import (
	"github.com/jmoiron/sqlx"
	"github.com/fefelovgroup/ratchet/data"
	"github.com/fefelovgroup/ratchet/logger"
	"github.com/fefelovgroup/ratchet/util"
)

// SQLiteWriter handles INSERTing data.JSON into a
// specified SQL table. If an error occurs while building
// or executing the INSERT, the error will be sent to the killChan.
//
// Note that the data.JSON must be a valid JSON object or a slice
// of valid objects, where the keys are column names and the
// the values are the SQL values to be inserted into those columns.
//
// For use-cases where a SQLiteWriter instance needs to write to
// multiple tables you can pass in SQLWriterData.
type SQLiteWriter struct {
	writeDB          *sqlx.DB
	TableName        string
	OnDupKeyUpdate   bool
	PrimaryKeys      []string
	PreservedFields  []string
	ConcurrencyLevel int // See ConcurrentDataProcessor
	BatchSize        int
}

// NewSQLiteWriter returns a new SQLiteWriter
func NewSQLiteWriter(db *sqlx.DB, tableName string) *SQLiteWriter {
	return &SQLiteWriter{writeDB: db, TableName: tableName, OnDupKeyUpdate: true, BatchSize:100}
}

// ProcessData defers to util.SQLiteInsertData
func (s *SQLiteWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// handle panics a bit more gracefully
	defer func() {
		if err := recover(); err != nil {
			util.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	// First check for SQLWriterData
	var wd SQLWriterData
	err := data.ParseJSON(d, &wd)
	logger.Info("SQLiteWriter: Writing data...")
	if err == nil && wd.TableName != "" && wd.InsertData != nil {
		logger.Debug("SQLiteWriter: SQLWriterData scenario")
		dd, err := data.NewJSON(wd.InsertData)
		util.KillPipelineIfErr(err, killChan)
		err = util.SQLiteInsertData(s.writeDB, dd, wd.TableName, s.OnDupKeyUpdate, s.PrimaryKeys, s.PreservedFields, s.BatchSize)
		util.KillPipelineIfErr(err, killChan)
	} else {
		logger.Debug("SQLiteWriter: normal data scenario")
		err = util.SQLiteInsertData(s.writeDB, d, s.TableName, s.OnDupKeyUpdate, s.PrimaryKeys, s.PreservedFields, s.BatchSize)
		util.KillPipelineIfErr(err, killChan)
	}
	logger.Info("SQLiteWriter: Write complete")
}

// Finish - see interface for documentation.
func (s *SQLiteWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLiteWriter) String() string {
	return "SQLiteWriter"
}

// Concurrency defers to ConcurrentDataProcessor
func (s *SQLiteWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
