package util

import (
	"github.com/jmoiron/sqlx"
	"fmt"
	"strings"

	"github.com/fefelovgroup/ratchet/data"
	"github.com/fefelovgroup/ratchet/logger"
	"errors"
	"sort"
)

// SQLiteInsertData abstracts building and executing a SQL INSERT
// statement for the given Data object.
//
// Note that the Data must be a valid JSON object
// (or an array of valid objects all with the same keys),
// where the keys are column names and the
// the values are SQL values to be inserted into those columns.
//
// If onDupKeyUpdate is true, then primaryKeys can be set.
// primaryKeys is used to lookup existing values for preservedFields.
// Fields specified as preserved will keep their current value
//
func SQLiteInsertData(db *sqlx.DB, d data.JSON, tableName string,
onDupKeyUpdate bool, primaryKeys[]string, preservedFields []string,
batchSize int) error {

	if len(preservedFields) > 0 {
		if len(primaryKeys) == 0 {
			return errors.New(
				"primaryKeys required if preservedFields specified")
		}
	}

	objects, err := data.ObjectsFromJSON(d)
	if err != nil {
		return err
	}
	tx:=db.MustBegin()
	if batchSize > 0 {
		for i := 0; i < len(objects); i += batchSize {
			maxIndex := i + batchSize
			if maxIndex > len(objects) {
				maxIndex = len(objects)
			}
			err = sqliteInsertObjects(tx, objects[i:maxIndex], tableName,
				onDupKeyUpdate, primaryKeys, preservedFields)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
		tx.Commit()
		return nil
	}
	tx.Commit()
	return sqliteInsertObjects(tx, objects, tableName, onDupKeyUpdate,
		primaryKeys, preservedFields)

}

func sqliteInsertObjects(tx *sqlx.Tx, objects []map[string]interface{},
tableName string, onDupKeyUpdate bool, primaryKeys[]string,
preservedFields []string) error {

	logger.Info(
		"SQLiteInsertData: building INSERT for len(objects) =", len(objects))
	insertSQL, vals, err := buildSQLiteInsertSQL(objects, tableName, onDupKeyUpdate,
		primaryKeys, preservedFields)
	if err != nil {
		return err
	}

	logger.Debug("SQLiteInsertData:", insertSQL)
	logger.Debug("SQLiteInsertData: values", vals)
	stmt, err := tx.Preparex(insertSQL)

	if err != nil {
		logger.Debug("SQLiteInsertData: error preparing SQL")
		return err
	}
	defer stmt.Close()

	res, err := stmt.Exec(vals...)
	if err != nil {
		return err
	}
	lastID, err := res.LastInsertId()
	if err != nil {
		return err
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		return err
	}

	logger.Info(
		fmt.Sprintf(
			"SQLiteInsertData: rows affected = %d, last insert ID = %d",
			rowCnt, lastID))
	return nil
}

func buildSQLiteInsertSQL(objects []map[string]interface{}, tableName string,
onDupKeyUpdate bool, primaryKeys[]string, preservedFields []string) (
insertSQL string, vals []interface{}, err error) {

	cols := sortedColumns(objects)

	// preservedFieldMap must be listed in cols,
	// regardless if they are present in the objects
	colMap := map[string]bool{}
	preservedFieldMap := map[string]bool{}
	primaryKeyMap := map[string]bool{}
	for _, c := range cols {
		colMap[c] = true
	}
	for _, pf := range preservedFields {
		preservedFieldMap[pf] = true
		// Add preservedField to cols?
		if !colMap[pf] {
			cols = append(cols, pf)
		}
	}
	sort.Strings(cols)
	for _, pk := range primaryKeys {
		primaryKeyMap[pk] = true
	}

	// Format: INSERT INTO tablename(col1,col2) VALUES(?,?),(?,?)
	// Select statements are used to determine
	// the current values of preservedFields columns
	// as explained here http://stackoverflow.com/a/4330694/639133
	if (onDupKeyUpdate) {
		insertSQL = fmt.Sprintf("INSERT OR REPLACE INTO %v(%v) VALUES",
			tableName, strings.Join(cols, ","))
	} else {
		// Do not update existing fields, just insert.
		// "ON CONFLICT" as specified by the create table statement
		// will determine the behaviour for duplicate keys
		// https://sqlite.org/lang_conflict.html
		insertSQL = fmt.Sprintf("INSERT INTO %v(%v) VALUES", tableName,
			strings.Join(cols, ","))
	}

	// Selected statements used to lookup existing values may require
	// some values to be bound to multiple placeholders.
	// This array specified how to find th values
	var valCols []string

	// builds the (?,?) part
	qs := "("
	for i := 0; i < len(cols); i++ {
		if i > 0 {
			qs += ","
		}
		if onDupKeyUpdate && preservedFieldMap[cols[i]] {
			// Do not update this field,
			// preserve current value,
			// or use default for new rows
			qs += fmt.Sprintf("(SELECT %v FROM %v WHERE ", cols[i], tableName)
			for k := 0; k < len(primaryKeys); k++ {
				if k > 0 {
					qs += "AND "
				}
				qs += fmt.Sprintf("%v = ?", primaryKeys[k])
				valCols = append(valCols, primaryKeys[k])
			}
			qs += ")"

		} else {
			// This field will be updated
			qs += "?"
			valCols = append(valCols, cols[i])
		}
	}
	qs += ")"
	// append as many (?,?) parts as there are objects to insert
	for i := 0; i < len(objects); i++ {
		if i > 0 {
			insertSQL += ","
		}
		insertSQL += qs
	}

	vals = []interface{}{}
	for _, obj := range objects {
		for _, col := range valCols {
			if val, ok := obj[col]; ok {
				vals = append(vals, val)
			} else {
				if primaryKeyMap[col] {
					err = errors.New(
						fmt.Sprintf("Missing value for primary key: %v", col))
					return
				}
				vals = append(vals, nil)
			}
		}
	}

	err = nil
	return
}
