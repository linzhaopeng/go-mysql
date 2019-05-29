package orm

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
	"sync"
)

type Database struct {
	User string
	Password string
	IP string
	Port string
	DbName string
	Charset string
}

type Table struct {
	Name string
	Db *sql.DB
}

var (
	DbPool map[string]*sql.DB
	connMu sync.RWMutex
)

func init() {
	DbPool = make(map[string]*sql.DB)
}

func RegisterDb(driverName, dbName string, database Database) (err error) {
	connMu.RLock()
	db, ok := DbPool[dbName]
	connMu.RUnlock()

	if ok {
		err = db.Ping()
		return
	}

	//connMu.Lock()
	//defer connMu.Unlock()
	DbPool[dbName] = &sql.DB{}
	dataSourceName := getDataSourceName(database)
	db, err = sql.Open(driverName, dataSourceName)
	if err == nil {
		DbPool[dbName] = db
	}
	return
}

func getDataSourceName(database Database) (dataSourceName string) {
	if database.Charset == "" {
		database.Charset = "utf8"
	}

	dataSourceName = database.User +
		":" +
		database.Password +
		"@tcp(" +
		database.IP +
		":" +
		database.Port +
		")/" +
		database.DbName +
		"?charset=" +
		database.Charset

	return
}

func (table *Table) Insert(data *interface{}) (id int64, err error) {
	dataType := reflect.TypeOf(data)
	dataValue := reflect.ValueOf(data)
	var fieldArr []string
	for i := 0; i < dataType.NumField(); i ++ {
		fieldArr = append(fieldArr, "`" + dataType.Field(i).Name + "` = " + dataValue.Field(i).String())
	}

	sqlStr := "insert into " +
		table.Name +
		" set " +
		strings.Join(fieldArr, ", ")
	res, err := table.Db.Exec(sqlStr)
	if err != nil {
		return 0, err
	}
	id, err = res.LastInsertId()
	return
}

func (table *Table) BatchInsert(data ...interface{}) (rows int64, err error) {
	dataType := reflect.TypeOf(data[0])
	var fieldArr []string
	for i := 0; i < dataType.NumField(); i ++ {
		fieldArr = append(fieldArr, dataType.Field(i).Name)
	}

	sqlStr := "insert into " +
		table.Name +
		"(`" +
		strings.Join(fieldArr, "`, `") +
		"`) value(?" +
		strings.Repeat(", ?", len(fieldArr) - 1) +
		")"

	res, err := table.Db.Exec(sqlStr, data)
	if err != nil {
		return 0, err
	}
	rows, err = res.LastInsertId()
	return
}