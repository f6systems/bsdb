package bsdb

import (
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

var (
	BS bool
)

func init() {
	log.Printf("[bsdb:init()] - Starting.\n")
	BS = false //bool used if BS steps run
}

//BSCheck - Check to see if the database has the bootstrap table
func BSCheck(dbc *sql.DB) error {
	log.Printf("<TEMP> In bsdb.BSCheck()\n")
	var err error
	if err = dbc.Ping(); err != nil {
		log.Printf("ERROR: Ping of sqlite database failed. ", err)
		return err
	} else {
		log.Printf("<INFO> [bsdb.BSCheck()] The ping response had *no* reported error.\n")
	}
	//bootstrapCreate() //create if not (err if can not ..)
	err = bootstrapCreate(dbc)
	if err != nil {
		log.Printf("<FAIL> Some ISSUE: Should *not* be here. (err=%v)\n", err)
	}
	BS = true
	log.Printf("<TEMP> Setting BS to = %v.\n", BS)
	return nil
}

//BSUpdate() - Get current to the .sql found in the provided sql/.
//Here parse the YYYYMMDDNN and run appropriate files into the database.

//BSRelease() - Return current BSRelease value (for now (feedback) an int as can compare...)
func BSRelease(dbc *sql.DB) (id int) {
	if err := dbc.Ping(); err != nil {
		log.Printf("ERROR: Ping of sqlite database failed. ", err)
		return 128
	}
	sQ := "SELECT Max(id) from bootstrap"
	/*
		  err := con.QueryRow("select * from users where user_id=?",id).Scan(&ReadUser.ID, &ReadUser.Name, &ReadUser.First, &ReadUser.Last, &ReadUser.Email )
	  switch {
	      case err == sql.ErrNoRows:
	              fmt.Fprintf(w,"No such user")
	      case err != nil:
	              log.Fatal(err)
	      default:
	        output, _ := json.Marshal(ReadUser)
	        fmt.Fprintf(w,string(output))
	  }	*/
	err := dbc.QueryRow(sQ).Scan(&id)
	if err != nil {
		//log.Printf("<FAIL> ERROR: Some issue with db query row. (err=%v)\n", err)
		//NOTE: Here , ping passed so there is most likely *no* rows .. return 0
		return 0
	}
	return id
	//return 0

}

//bootstrapCreate() - Instansiate the bootstrap table
func bootstrapCreate(dbc *sql.DB) error {
	//TODO:(hopley) - A func dbPing()? for all?
	if err := dbc.Ping(); err != nil {
		log.Printf("ERROR: Ping of sqlite database failed. ", err)
		return err
	}
	sql_table := `
        CREATE TABLE IF NOT EXISTS bootstrap (
	   id INT NOT NULL,
	   updatedAt TIMESTAMP
        );
        `
	_, err := dbc.Exec(sql_table)
	if err != nil {
		log.Printf("<FAIL> PANIC - Creating bootstrap table. (err=%v)\n", err)
		panic(err) //? should pass this back?
	}
	return nil
}

// need ; to be called from main ; have latest from bootstrap table;
//  read dir of the .sql files and build a map of the > max(id) and not 00
//  pass result with *sql.DB and process all needed YYYYMMDDNN.sql files ...

//
func getSQLFiles(d string) error { //BSFileDir(dir string/os.path ...)
	err := dirExists(d)
	if err != nil {
		d = "./sql"
		//d = "/home/hopley/go/src/github.com/f6systems/testbs/sql"
		err = dirExists(d)
		if err != nil {
			return err
		}
	}
        files, _ := ioutil.ReadDir(d)
        for _, f := range files {
                log.Println("File=" + f.Name())
        }
	return nil
}

func dirExists(d string) error {
        Dir := os.Getenv(d)
	_,err :=  os.Stat(Dir)
        if err != nil {
                log.Printf("<FAIL> Issue directory,%q, does not exist. (err=%v).\n",d,err)
                return err
        }
	return nil
}

func getNewerSQL(id int,dir string) { //TODO:(hopley) create an appropriate struct for return ...
        //log.Printf("<INFO> Looking for files that are >%d. (and *not* YYYYMMDD00.sql...)\n",id)
        files, _ := ioutil.ReadDir(dir)
        for _, f := range files {
                ok,err := filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][1-9].sql",f.Name())
                if err != nil {
                        log.Printf("<FAIL> ERROR getting listing. (err=%v)\n",err)
                }
                if ok {
                        log.Println("File=" + f.Name())
                }
                ok,err = filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][1-9][0-9].sql",f.Name())
                if ok {
                        log.Println("File=" + f.Name())
                }
        }

}

/*
 bootstrap table
  Plan to be a key/value with Key the YYYYMMDDNN and Value DateTimeStamp
   Can sort on Key and use DateTimeStamp as logging AND with a null value if Failed...

   CREATE Table bootstrap (
	   id INT NOT NULL,
	   updatedAt TIMESTAMP
   );

*/

/*
 TODO:(hopley) - set up environment for BS_DBDIR & BS_SQLDIR (where the YYYYMMDDNN.sql lives)
 TODO:(hopley) - later, a BD_DBDRIVER (mysql, sqlite, postgres ...)

 os.Getenv("BS_DBDIR
 os.Getenv("BS_SQLDIR")

*/
