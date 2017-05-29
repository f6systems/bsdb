package bsdb

import (
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	BS bool
)

func init() {
	log.Printf("[bsdb:init()] - Starting.\n")
	BS = false //bool used if BS steps run
}

//Bootstrap - Use provided startng value to run in .sql files from DB_DIR to get to most recent
func Bootstrap(conn *sql.DB, dir string) error {
	ver := BSRelease(conn)
	log.Printf("<INFO> Current database, provide, notes current release at=%d\n", ver)
	if ver == 0 {
		sqlSchema := getNewestSQL00(dir)
		log.Printf("<INFO> Needed a schema file (YYYYMMDD00.sql) and here it is=%d.\n", sqlSchema)
		os.Exit(0)
		/*
			sqlFile := dir + "/" + sqlSchema + ".sql"
			err := BSExec(sqlFile)
			if err != nil {
				log.Printf("<FAIL> Issue in Exec of most recent(sqlSchema=%d) schema.\n",sqlSchema)
				return err
			}
		ver = BSRelease(conn)
		*/

	} else {
        //TODO:(hopley) review golang array sort to ensure file names get sorted as expected
	files, _ := ioutil.ReadDir(dir)
        for _, f := range files {
                //ok,err := filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][1-9].sql",f.Name())
                ok, err := filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0][1-9].sql", f.Name())
                if err != nil {
                        log.Printf("<FAIL> ERROR getting listing. (err=%v)\n", err)
                }
                if ok {
                        //log.Println("Process this file=" + f.Name() + " zero file")
                        if isFileGreater(ver, f.Name()) {
				log.Printf("<INFO> NOTE: Should call processFile0(%s).\n",f.Name())
                                //processFile(f.Name())
				ver = BSRelease(conn)

                        }
                }
                ok, err = filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][1-9][0-9].sql", f.Name())
                if ok {
                        //log.Println("Process this file=" + f.Name())
                        if isFileGreater(ver, f.Name()) {
				log.Printf("<INFO> NOTE: Should call processFile(%s).\n",f.Name())
                                //processFile(f.Name())
				ver = BSRelease(conn)
                        }
                }
        }


	}
	return nil
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
	//func getNewerSQL(id int,dir string) { //TODO:(hopley) create an appropriate struct for return ...
	err := dbc.QueryRow(sQ).Scan(&id)
	log.Printf("<INFO> The result of the ID from the database=%v.\n", id) //id is an int
	if err != nil { // no entries, so get largest/most recent YYYYMMDD00.sql file ...
		log.Printf("<FAIL> ERROR: Some issue with db query row. (err=%v)\n", err)
		//NOTE: Here , ping passed so there is most likely *no* rows .. return 0
		log.Printf("<INFO> Need to get most recent YYYYMMDD00.sql ... getNewestSQL00('./sql').\n")
		//getNewestSQL00(dir)
		createFile00 := getNewestSQL00("./sql")
		log.Printf("Newest 00 file = %d.\n", createFile00)
		//return createFile00
		//<TEMP>
		id = createFile00
		//return 0
	}
		//log.Printf("Newest 00 file = %d.\n", id)
	//getNewerSQL(id, "./sql")
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
	_, err := os.Stat(Dir)
	if err != nil {
		log.Printf("<FAIL> Issue directory,%q, does not exist. (err=%v).\n", d, err)
		return err
	}
	return nil
}

func getNewerSQL(id int, dir string) { //TODO:(hopley) create an appropriate struct for return ...
	//log.Printf("<INFO> Looking for files that are >%d. (and *not* YYYYMMDD00.sql...)\n",id)
	files, _ := ioutil.ReadDir(dir)
	//TODO:(hopley) review golang array sort to ensure file names get sorted as expected
	for _, f := range files {
		//ok,err := filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][1-9].sql",f.Name())
		ok, err := filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0][1-9].sql", f.Name())
		if err != nil {
			log.Printf("<FAIL> ERROR getting listing. (err=%v)\n", err)
		}
		if ok {
			log.Println("Process this file=" + f.Name() + " zero file")
			if isFileGreater(id, f.Name()) {
				//processFile(f.Name())
			}
		}
		ok, err = filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][1-9][0-9].sql", f.Name())
		if ok {
			log.Println("Process this file=" + f.Name())
			if isFileGreater(id, f.Name()) {
				//processFile(f.Name())
			}
		}
	}

}

//
func getNewestSQL00(dir string) int {
	file0 := 1970010100
	file00 := 1970010100
	log.Printf("<INFO> - Here do a sort from dir(%q) and get largest 00.sql.\n", dir)
	log.Printf("<INFO> Need  the specific sort from the listing ...\n")
	//ReadDir reads the directory named by dirname and returns a list of directory entries sorted by filename.
	files, _ := ioutil.ReadDir(dir)
	log.Printf("<INFO> Number of 00 files=%d.\n", len(files))
	//TODO:(hopley) review golang array sort to ensure file names get sorted as expected
	for k, f := range files {
		//sort.Ints(arr)
		ok, err := filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0][0].sql", f.Name())
		if err != nil {
			log.Printf("<FAIL> ERROR getting listing. (err=%v)\n", err)
		}
		if ok {
			//fileName := strings.TrimSuffix(filepath.Ext(f.Name()),".sql")
			fileName := strings.TrimSuffix(f.Name(), filepath.Ext(f.Name()))
			log.Printf("<INFO> Here fileName=%s.\n", fileName)
			file0, _ = strconv.Atoi(fileName)
			log.Printf("00File[%d](file0)=%d.(file00=%d)", k, file0, file00)
			if file0 > file00 {
				file00 = file0
			}
		}
		/*
		   ok,err = filepath.Match("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0][0].sql",f.Name())
		   if ok {
		           log.Println("00File=" + f.Name())
		   }
		*/
	}
	return file00
}

//
func isFileGreater(ver int, file string) bool {
	log.Printf("<INFO> isFileGreater() ver=%d and file = %s.\n", ver, file)
			fileName := strings.TrimSuffix(file, filepath.Ext(file))
			log.Printf("<INFO> Here fileName=%s.\n", fileName)
			fileN, _ := strconv.Atoi(fileName)
			if fileN > ver {
				return true
			}
	return false
}

//TODO:(hopley) - func for execSQL to run in the new SQL files.

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
