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
	Server   = "localhost"
	Port     = 5432
	Database = "greenely"
	User     = "breamio"
	Password = "Breamhack13"
	SSL      = "disable"
)

type Storer interface {
	Store(interface{}) error
	io.Closer
}

var db *sql.DB

func DB() *sql.DB {
	return db
}

func init() {
	connstring := fmt.Sprintf("host='%s' port='%d' user='%s' password='%s' dbname='%s' sslmode='%s'",
		Server, Port, User, Password, Database, SSL)

	fmt.Println(connstring)

	var err error
	db, err = sql.Open("postgres", connstring)

	if err != nil {
		panic(err)
	}
}

func GenerateStorer(table string, model interface{}) (Storer, error) {
	base := "INSERT INTO %s (%s) VALUES (%s);"

	typ := reflect.TypeOf(model)
	if typ.Kind() != reflect.Struct {
		panic(errors.New(fmt.Sprintf("Wrong model type for storer. Expected %s, got %s.", reflect.Struct, typ.Kind())))
	}
	
	columns := []string{}
	params := []string{}

	mapper := make(map[int]string)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var column_name string
		if tagname := field.Tag.Get("db"); tagname != "" {
			column_name = tagname
		} else {
			column_name = field.Name
		}
		columns = append(columns, column_name)
		params = append(params, fmt.Sprintf("$%d", i+1))
		mapper[i] = field.Name
	}

	query := fmt.Sprintf(base, pq.QuoteIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(params, ", "))

	stmt, err := DB().Prepare(query)
	
	if err != nil {
		return nil, err
	}
	
	return &dbStorer{
		numArgs: len(params),
		mapper: mapper,
		query: query,
		Stmt: stmt,
	}, nil
}

type dbStorer struct {
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
	params := make([]interface{}, 0, s.numArgs)
	for i := 0; i < s.numArgs; i++ {
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
