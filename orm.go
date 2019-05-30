package orm

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strconv"
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

func getFieldValue(v reflect.Value) (value string) {
	switch v.Kind() {
	case reflect.Invalid:
		value = "invalid"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value = strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value = strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		value = strconv.FormatFloat(v.Float(), 'f', 2, 64)
	case reflect.String:
		value = v.String()
	case reflect.Bool:
		value = strconv.FormatBool(v.Bool())
	default:
		value = v.Type().String() + " value"
	}
	return
}

func (table *Table) Insert(data interface{}) (id int64, err error) {
	dataType, dataValue := reflect.TypeOf(data), reflect.ValueOf(data)
	fieldNum := dataType.NumField()
	var (
		fieldName string
		fieldArr []string
		valueArr []interface{}
	)
	for i := 0; i < fieldNum; i ++ {
		if fieldName = dataType.Field(i).Tag.Get("name"); fieldName == "" {
			fieldName = dataType.Field(i).Name
		}
		fieldArr = append(fieldArr, fieldName)

		valueArr = append(valueArr, getFieldValue(dataValue.Field(i)))
	}

	sqlStr := "insert into " +
		table.Name +
		" set `" +
		strings.Join(fieldArr, "`= ?, `") +
		"` = ?"
	stmt, err := table.Db.Prepare(sqlStr)
	if err != nil {
		return 0, err
	}
	res, err := stmt.Exec(valueArr...)
	if err != nil {
		return 0, err
	}
	id, err = res.LastInsertId()
	return
}

func (table *Table) BatchInsert(data interface{}) (rows int64, err error) {
	sind := reflect.Indirect(reflect.ValueOf(data))
	firstData := sind.Index(0).Interface()
	dataType := reflect.TypeOf(firstData)
	var (
		fieldArr []string 
		fieldName string
		valueArr []string
	)
	for i := 0; i < dataType.NumField(); i ++ {
		if fieldName = dataType.Field(i).Tag.Get("name"); fieldName == "" {
			fieldName = dataType.Field(i).Name
		}
		fieldArr = append(fieldArr, fieldName)
	}

	/*valueChannel := make(chan interface{})
	var wg sync.WaitGroup*/
	for i := 0; i < sind.Len(); i ++ {
		/*wg.Add(1)
		go func(data interface{}) {
			defer wg.Done()
			var valueArr []interface{}
			dataValue := reflect.ValueOf(data)
			for i := 0; i < dataValue.NumField(); i ++ {
				valueArr = append(valueArr, getFieldValue(dataValue.Field(i)))
			}
			valueChannel<-valueArr
		}(data[i])*/
		var values []string
		dataValue := reflect.ValueOf(sind.Index(i).Interface())
		for i := 0; i < dataValue.NumField(); i ++ {
			values = append(values, getFieldValue(dataValue.Field(i)))
		}
		valueArr = append(valueArr, "('" + strings.Join(values, "', '") + "')")
	}

	/*go func() {
		wg.Wait()
		close(valueChannel)
	}()

	for value := range valueChannel {
		valueArr = append(valueArr, value)
	}*/
	sqlStr := "insert into " +
		table.Name +
		"(`" +
		strings.Join(fieldArr, "`, `") +
		"`) value"  +
		strings.Join(valueArr, ",")
	res, err := table.Db.Exec(sqlStr)
	if err != nil {
		return 0, err
	}
	rows, err = res.RowsAffected()
	return
}