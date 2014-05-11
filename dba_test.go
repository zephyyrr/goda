package goda

import (
	"database/sql"
	"testing"
	"time"
	"math"
)

type testStruct struct {
	Id        int
	Real      float64
	Sträng    string `db:"name"`
	Timestamp time.Time
}

var dbcd = DBConnectData{"localhost", PostgresPort, "testing", "tester", "test", SSLDisable}

func TestDBAStorer(t *testing.T) {
	dba, err := NewDatabaseAdministrator(dbcd)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dba.Storer("dbatest", testStruct{})
	if err == nil {
		t.Error("Should fail with non-existing table.")
	}

	setup(dba.DB)
	defer cleanup(dba.DB)
	storer, err := dba.Storer("dbatest", testStruct{})
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Now()
	err = storer.Store(testStruct{90, 42.1337, "H.E.L.L", ts})
	if err != nil {
		t.Error("Expected nil, got", err)
	}
	row := dba.QueryRow("SELECT real, name, timestamp FROM dbatest where id=90;")
	if err != nil {
		t.Error("Error querying.", err)
	}
	res := testStruct{}
	err = row.Scan(&(res.Real), &(res.Sträng), &(res.Timestamp))
	if err != nil {
		t.Error(err)
	}
	if math.Float64bits(res.Real) == math.Float64bits(42.1338) {
		t.Error(res.Real, "!=", 42.1337)
	}
	if res.Sträng != "H.E.L.L" {
		t.Error(res.Sträng, "!=", "H.E.L.L")
	}
	if res.Timestamp.Equal(ts) {
		t.Error(res.Timestamp, "!=", ts)
	}
}

func TestDBARetriever(t *testing.T) {
	dba, err := NewDatabaseAdministrator(dbcd)
	if err != nil {
		t.Fatal(err)
	}
	setup(dba.DB)
	defer cleanup(dba.DB)
	ret, err := dba.Retriever("dbatest", testStruct{}, map[string]interface{}{"id":17})
	if err != nil {
		t.Error(err)
	}
	
	t.Log(ret)
	
	var res testStruct
	
	err = ret.Retrieve(&res)
	if err != nil {
		t.Error(err)
	}
	
	if res.Id != 17 { t.Error(res.Id, "!=", 17) }
	if res.Real != 12.5 { t.Error(res.Real, "!=", 12.5) }
	if res.Sträng != "AAA" { t.Error(res.Sträng, "!=", "AAA") }
}

func setup(db *sql.DB) {
	db.Exec("CREATE TABLE dbatest (id integer PRIMARY KEY, real real, name varchar(30), timestamp timestamp with time zone);")
	db.Exec("INSERT INTO dbatest (id, real, name, timestamp) VALUES (17, 12.5, 'AAA', 'now')")
}

func cleanup(db *sql.DB) {
	db.Exec("drop table dbatest;")
	//fmt.Println("Dropped table.")
}
