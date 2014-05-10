package dba

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"strings"
	"errors"
	
	"github.com/lib/pq"
)

const (
	insertBase = "INSERT INTO %s (%s) VALUES (%s);"
)

const (
	SSLDisable = "disable"
	SSLRequire = "require"
)

const (
	PostgresPort = 5432
)

type DBConnectData (
	Server   string
	Port     int
	Database string
	User     string
	Password string
	SSL string
)

type DatabaseAdministrator {
	*sql.DB
	storers map[reflect.Type]Storer
}

func NewDatabaseAdministrator(dbcd DBConnectData) (*DatabaseAdministrator, error) {
	connstring := fmt.Sprintf("host='%s' port='%d' user='%s' password='%s' dbname='%s' sslmode='%s'",
		dbcd.Server, dbcd.Port, dbcd.User, dbcd.Password, dbcd.Database, dbcd.SSL)

	fmt.Println(connstring)

	var err error
	db, err = sql.Open("postgres", connstring)

	return &DatabaseAdministrator{db}, err
}

func (dba *DatabaseAdministrator) Storer(table string, model interface{}) (Storer, error) {
	typ := reflect.TypeOf(model)
	if st, ok := dba.storers[typ]; ok {
		return st
	}
	
	if typ.Kind() != reflect.Struct {
		panic(errors.New(fmt.Sprintf("Wrong model type for storer. Expected %s, got %s.", reflect.Struct, typ.Kind())))
	}

	columns, mapper := dbfields(typ);
	params := []string{}
	for i, _ := range columns {
		params = append(params, fmt.Sprintf("$%d", i+1))
	}

	query := queryBuilder(insertBase, table, columns, params)
	stmt, err := DB().Prepare(query)
	
	if err != nil {
		return nil, err
	}
	
	dba.storers[typ] = &stmtStorer{
		numArgs: len(params),
		mapper: mapper,
		query: query,
		Stmt: stmt,
	}, nil
	
	return dba.storers[typ]
}

type stmtStorer struct {
	numArgs int
	mapper map[int]string
	query string
	*sql.Stmt
}

func (s *dbStorer) Store(data interface{}) error {
	val := reflect.ValueOf(data)
	for val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Type().Kind() != reflect.Struct {
		panic(errors.New("Type is not struct or pointer to struct."))
	}
	//fmt.Println("storing", val.Type(), data)
	columns, mapper := dbfields(typ)
	
	params := make([]interface{}, 0, s.numArgs)
	for i, _ := range mapper {
		params = append(params, val.FieldByName(s.mapper[i]).Interface())
	}
	
	_, err := s.Exec(params...)
	
	return err
}

func (s *dbStorer) String() string {
	return s.query
}

func Close() {
	db.Close()
}

func dbfields(reflect.Type typ) ([]string, map[int]string) {
	fields := []string{}

	mapper := make(map[int]string)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var column_name string
		if tagname := field.Tag.Get("db"); tagname != "" {
			column_name = tagname
		} else {
			column_name = field.Name
		}
		fields = append(fields, column_name)
		mapper[i] = field.Name
	}
	
	return fields, mapper
}

func queryBuilder(base, table string, columns, params []string) {
	return fmt.Sprintf(base, pq.QuoteIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(params, ", "))
}
