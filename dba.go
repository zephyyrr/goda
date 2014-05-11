package goda

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
)

const (
	insertBase = "INSERT INTO %s (%s) VALUES (%s);"
	selectBase = "SELECT (%s) FROM %s WHERE %s;"
)

const (
	SSLDisable = "disable"
	SSLRequire = "require"
	SSLVerify  = "verify-full"
)

const (
	PostgresPort = 5432
)

type DBConnectData struct {
	Server   string
	Port     int
	Database string
	User     string
	Password string
	SSL      string
}

func (dbcd DBConnectData) String() string {
	if dbcd.SSL == "" {
		dbcd.SSL = SSLRequire
	}
	s := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		dbcd.Server,
		dbcd.Port,
		dbcd.User,
		dbcd.Password,
		dbcd.Database,
		dbcd.SSL)
	return s
}

type DatabaseAdministrator struct {
	*sql.DB
	storers map[reflect.Type]Storer
}

func NewDatabaseAdministrator(dbcd DBConnectData) (*DatabaseAdministrator, error) {
	fmt.Println(dbcd.String())

	db, err := sql.Open("postgres", dbcd.String())
	if err != nil {
		return nil, err
	}
	err = db.Ping() // Make sure connection can be established.
	return &DatabaseAdministrator{db, make(map[reflect.Type]Storer)}, err
}

func (dba *DatabaseAdministrator) Close() {
	for typ, _ := range dba.storers {
		delete(dba.storers, typ)
	}
	dba.Close()
}

func (dba *DatabaseAdministrator) Storer(table string, model interface{}) (Storer, error) {
	typ := reflect.TypeOf(model)
	if st, ok := dba.storers[typ]; ok {
		return st, nil
	}

	if typ.Kind() != reflect.Struct {
		panic(errors.New(fmt.Sprintf("Wrong model type for storer. Expected %s, got %s.", reflect.Struct, typ.Kind())))
	}

	columns, mapper := dbfields(typ)
	params := []string{}
	for i, _ := range columns {
		params = append(params, fmt.Sprintf("$%d", i+1))
	}

	query := fmt.Sprintf(insertBase, pq.QuoteIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(params, ", "))

	// fmt.Println(query)
	stmt, err := dba.Prepare(query)

	if err != nil {
		return nil, err
	}

	dba.storers[typ] = &stmtStorer{
		mapper: mapper,
		query:  query,
		Stmt:   stmt,
	}

	return dba.storers[typ], nil
}

func (dba *DatabaseAdministrator) Retriever(table string, model interface{}, keys map[string]interface{}) (Retriever, error) {
	typ := reflect.TypeOf(model)
	if typ.Kind() != reflect.Struct {
		panic(errors.New(fmt.Sprintf("Wrong model type for retriever. Expected %s, got %s.", reflect.Struct, typ.Kind())))
	}

	columns, mapper := dbfields(typ)
	params := []string{}
	for key, val := range keys {
		params = append(params, fmt.Sprintf("%s=%v", key, val))
	}

	// Select (fields in model) from table where key=val;
	query := fmt.Sprintf(selectBase,
		strings.Join(columns, ", "),
		pq.QuoteIdentifier(table),
		strings.Join(params, ", "))

	rows, err := dba.Query(query)
	if err != nil {
		return nil, err
	}

	return &dbRetriever{
		mapper: mapper,
		query:  query,
		Rows:   rows,
	}, nil
}

type stmtStorer struct {
	mapper map[int]string
	query  string
	*sql.Stmt
}

func (s *stmtStorer) Store(data interface{}) error {
	val := reflect.ValueOf(data)
	for val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Type().Kind() != reflect.Struct {
		panic(errors.New("Type is not struct or pointer to struct."))
	}
	//fmt.Println("storing", val.Type(), data)
	_, mapper := dbfields(val.Type())

	params := make([]interface{}, len(s.mapper), len(s.mapper))
	for i, fieldName := range mapper {
		params[i] = val.FieldByName(fieldName).Interface()
	}

	_, err := s.Exec(params...)
	return err
}

func (s *stmtStorer) String() string {
	return s.query
}

type dbRetriever struct {
	mapper map[int]string
	query  string
	*sql.Rows
}

func (r *dbRetriever) Retrieve(data interface{}) error {
	fields := make([]interface{}, len(r.mapper), len(r.mapper))
	val := reflect.ValueOf(data)
	for val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Type().Kind() != reflect.Struct {
		panic(errors.New("Type is not struct or pointer to struct."))
	}
	
	for i, fieldName := range r.mapper {
		fields[i] = val.FieldByName(fieldName).Addr().Interface()
	}

	if r.Next() {
		fmt.Printf("%p\n", data)
		fmt.Println(fields)
		r.Scan(fields...)
		fmt.Println(data)
	} else {
		return errors.New("End of rows.")
	}
	return nil
}

func (r *dbRetriever) String() string {
	return r.query
}

func dbfields(typ reflect.Type) ([]string, map[int]string) {
	fields := []string{}

	mapper := make(map[int]string)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var column_name string
		if tagname := field.Tag.Get("db"); tagname != "" {
			column_name = tagname
		} else {
			column_name = strings.ToLower(field.Name)
		}
		fields = append(fields, pq.QuoteIdentifier(column_name))
		mapper[i] = field.Name
	}

	return fields, mapper
}
