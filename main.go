package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	_ "github.com/lib/pq"
)

var (
	logFileName string = "log_extChan_v1.log"
	wg                 = sync.WaitGroup{}
	connStr     string = "dbname = dbx host = redshift.com port = 1111 user = aaaaaa password = 1xxxxx"
	maxConcJobs string = "6"
	stepRows    string = "50000" 
	countOfRows string
)

func main() {

	fmt.Println(">> Starting..")

	////////////////////////////////
	// Setting the logging bases.
	////////////////////////////////

	logFile, err := os.OpenFile(LogFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	log.Println("; Starting execution;;;;;;;;")

	fmt.Println(">> Connecting to DB")

	////////////////////////////////
	// Starting extraction process.
	////////////////////////////////

	// Creating the connection with the DB.
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	log.Println("; Connected;;;;;;;;")
	fmt.Println(">> Got connected!")

	// Get the table's length.
	fmt.Println(">> Querying for the length of the table...")
	log.Println("; Querying the table's length;;;;;;;;")

	lengthTable, err := db.Query("SELECT COUNT(*) FROM table_name;")
	if err != nil {
		fmt.Println(err)
		log.Fatalln(err)
	}
	defer lengthTable.Close()

	for lengthTable.Next() {
		err := lengthTable.Scan(&countOfRows)
		if err != nil {
			fmt.Println(err)
			log.Fatalln(err)
		}
	}
	fmt.Println(">> Table's length is:", countOfRows)

	// Querying the table to get the number of columns and its names.
	queryingColumns := "SELECT * FROM table_name LIMIT 1;"

	descriptionRow, err := db.Query(queryingColumns)
	if err != nil {
		fmt.Printf(">> Querying the table failed: %v\n", err)
		log.Fatalln(err)
	}
	defer descriptionRow.Close()

	colNames, err := descriptionRow.Columns()
	if err != nil {
		fmt.Printf(">> Getting the cols error: %v\n", err)
		log.Fatalln(err)
	}
	fmt.Printf(">> Names of columns are: %v\n", colNames)
	fmt.Printf(">> Number of columns is: %v\n\n", len(colNames))
  
	numOfCols := len(colNames)

	// Defining chunks-slices and required values.
	countOfRowsNum, _ := strconv.Atoi(countOfRows)
	stepRowsNum, _ := strconv.Atoi(stepRows)
	maxConcJobsNum, _ := strconv.Atoi(maxConcJobs)
	serialNumberQuantity := countOfRowsNum / 365 

	chunkSizer := Slicer(serialNumberQuantity, stepRowsNum)
	numOfFiles := len(chunkSizer)
	workLoad := make(chan int, numOfFiles)
	workDone := make(chan int, numOfFiles)

	fmt.Printf("Slicer channel will have a length of %v with %v serialNumber in groups of %v\n",
		numOfFiles, serialNumberQuantity, stepRows)
	log.Printf("Slicer channel will have a length of %v with %v serialNumber in groups of %v\n",
		numOfFiles, serialNumberQuantity, stepRows)

	//////////////////////////////////////////////////
	// Query construction and concurrent extraction.
	//////////////////////////////////////////////////

	// Iterate to create the extractors
	for extractorID := 1; extractorID <= maxConcJobsNum; extractorID++ {
		log.Printf("; MAKING; extractor_%v;;;;;;;\n", extractorID)
		go ExtractorToCSV(db, extractorID, numOfCols, stepRowsNum, workLoad, workDone)
	}
	fmt.Println("Extractors created...")

	// Load the channel with its work to be done.
	for _, value := range chunkSizer {
		workLoad <- value
	}
	close(workLoad)
	log.Println("Channel loaded...")
	fmt.Println("Channel loaded...")

	// Unloading the channel from the work done.
	for fileDone := 1; fileDone <= numOfFiles; fileDone++ {
		<-workDone
	}
	log.Println("All process is done... END of program.")
	fmt.Println("All process is done... END of program.")

}

// Slicer creates a slice to guide the extraction process.
// Receive the length of the table and the number of rows to be contained in each chunk.
func Slicer(tableLen, chunkSize int) []int {

	length := tableLen / chunkSize

	if tableLen%chunkSize == 0 {
		chunks := make([]int, length)

		for index := range chunks {
			value := (index + 1) * chunkSize
			chunks[index] = value
		}
		return chunks
	}

	chunks := make([]int, length+1)

	for index := 0; index < length; index++ {
		value := (index + 1) * chunkSize
		chunks[index] = value
	}
	chunks[length] = tableLen
	return chunks // err needs to be added

}

// ExtractorToCSV extracts from a *DB to a CSV file.
// Creates a CSV file as specified, containing the result of the query.
func ExtractorToCSV(db *sql.DB, extractID, nCols, stepSize int, loadedCh, doneCh chan int) {

	var indexTracker, addNum, startingAt, endingAt int
	var queryToExtract, fileName string

	for load := range loadedCh {

		if load%stepSize != 0 {
			addNum = 1
		}

		indexTracker = (load / stepSize) - 1 + addNum
		startingAt = (indexTracker * stepSize) + 1
		endingAt = load

		queryToExtract = fmt.Sprintf("SELECT * "+
		"FROM table_name "+
		"WHERE serialNumber >= %v AND serialNumber <= %v", startingAt, endingAt)

		fileName = fmt.Sprintf("ChFile%v_ExportingRows_%v_%v.csv", indexTracker, startingAt, endingAt)
		log.Printf("; STARTING; extractor_%v; %v;;;;;;\n", extractID, fileName)
		fmt.Println("Ext", extractID, "STARTS with file", fileName)

		// Query the DB
		rows, err := db.Query(queryToExtract)
		if err != nil {
			fmt.Println(err)
			log.Println(err)
		}
		defer rows.Close()

		// Create a csv file to insert the data
		csvFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println(err)
			log.Println(err)
		}
		defer csvFile.Close()

		// Iterate over the rows of the query to create the string to be exported.
		for rows.Next() {

			tempValues := make([]interface{}, nCols)
			rowValues := make([]string, nCols)

			for i := 0; i < nCols; i++ {
				tempValues[i] = &rowValues[i]
			}

			err := rows.Scan(tempValues...)
			if err != nil {
				fmt.Println(err)
				log.Fatalln(err)
			}

			var stringValues string

			for i := 0; i < nCols-1; i++ {
				stringValues += rowValues[i] + ";"
			}
			stringValues += rowValues[nCols-1] + "\n"

			csvFile.WriteString(stringValues)

		}
		log.Printf("; ENDED; extractor_%v; %v;;;;;;\n", extractID, fileName)
		fmt.Println(">> Ext", extractID, "has END with file", fileName)
		doneCh <- load
	}
}
