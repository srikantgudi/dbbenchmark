package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

const (
	HOST     = "localhost"
	PORT     = 5432
	USER     = "postgres"
	PASSWORD = "toor"
	DBNAME   = "northwind"
)

func main() {
	st := time.Now()

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		HOST, PORT, USER, PASSWORD, DBNAME)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("Connected to PG")

	rowCount := 0

	rows, _ := db.Query("Select \"ProductID\", \"ProductName\", \"UnitPrice\", \"ReorderLevel\" From products")

	for rows.Next() {
		id := 0
		pname := ""
		unitprice := 0.0
		rorlevel := 0
		rows.Scan(&id, &pname, &unitprice, &rorlevel)
		rowCount++
		fmt.Printf("%4d %-40.40s %12.2f %10d\n", id, pname, unitprice, rorlevel)
	}
	customers, custerr := db.Query("Select \"CustomerID\", \"CompanyName\", \"City\", \"Country\" From customers")

	if custerr != nil {
		panic(custerr)
	}

	for customers.Next() {
		custid := ""
		custname := ""
		city := ""
		country := ""
		cscanerr := customers.Scan(&custid, &custname, &city, &country)
		if cscanerr != nil {
			panic(cscanerr)
		}
		rowCount++
		fmt.Printf("%-6s %-40.40s %-20s %-20s\n", custid, custname, city, country)
	}

	orders, orderr := db.Query("Select \"OrderID\", \"OrderDate\", \"RequiredDate\", \"ShippedDate\" From orders")

	if orderr != nil {
		panic(orderr)
	}
	for orders.Next() {
		var ordId int
		var ordDate sql.NullString
		var reqDate sql.NullString
		var shipDate sql.NullString
		oscanerr := orders.Scan(&ordId, &ordDate, &reqDate, &shipDate)
		if oscanerr != nil {
			panic(oscanerr)
		}
		rowCount++
		fmt.Printf("%5v %-20v %-20v %-20v\n", ordId, ordDate.String, reqDate.String, shipDate.String)
	}

	ordets, ordeterr := db.Query("Select od.\"OrderID\", p.\"ProductName\", od.\"Quantity\", od.\"UnitPrice\" From order_details od Join products p on p.\"ProductID\" = od.\"ProductID\"")

	if ordeterr != nil {
		panic(ordeterr)
	}
	for ordets.Next() {
		var ordId int
		var prodName sql.NullString
		var qty int
		var unitprice float64
		odscanerr := ordets.Scan(&ordId, &prodName, &qty, &unitprice)
		if odscanerr != nil {
			panic(odscanerr)
		}
		rowCount++
		fmt.Printf("%8d %-40v %10d %12.2f\n", ordId, prodName.String, qty, unitprice)
	}

	fmt.Printf("\n\nRunning inner Queries\n\n")

	rcustomers, rcusterr := db.Query("Select \"CustomerID\", \"CompanyName\", \"City\", \"Country\" From customers")

	if rcusterr != nil {
		log.Fatalf("ERR-Customer %s\n", rcusterr.Error())
	}

	for rcustomers.Next() {
		custid := ""
		custname := ""
		city := ""
		country := ""
		cscanerr := rcustomers.Scan(&custid, &custname, &city, &country)
		if cscanerr != nil {
			panic(cscanerr)
		}
		rowCount++
		fmt.Printf("\n%-6s %-40.40s %-20s %-20s\n", custid, custname, city, country)

		orders, orderr := db.Query("Select \"OrderID\", TO_CHAR(\"OrderDate\", 'dd-mm-yyyy') \"OrderDate\", TO_CHAR(\"RequiredDate\",'dd-mm-yyyy') RequiredDate, TO_CHAR(\"ShippedDate\",'dd-mm-yyyy') ShippedDate From orders Where \"CustomerID\" = $1", custid)

		if orderr != nil {
			log.Fatalf("inner queries - orders error: [%v]\n", orderr.Error())
		}
		for orders.Next() {
			var ordId int
			var ordDate sql.NullString
			var reqDate sql.NullString
			var shipDate sql.NullString
			oscanerr := orders.Scan(&ordId, &ordDate, &reqDate, &shipDate)
			if oscanerr != nil {
				panic(oscanerr)
			}
			rowCount++
			fmt.Printf("\n\t%5v %-20v %-20v %-20v\n\n", ordId, ordDate.String, reqDate.String, shipDate.String)

			ordets, ordeterr := db.Query("Select p.\"ProductName\", od.\"Quantity\", od.\"UnitPrice\" From order_details od Join products p on p.\"ProductID\" = od.\"ProductID\" Where od.\"OrderID\" = $1", ordId)

			if ordeterr != nil {
				log.Fatalf("inner queries - details for order: [%v]", ordeterr.Error())
			}
			for ordets.Next() {
				var prodName sql.NullString
				var qty int
				var unitprice float64
				odscanerr := ordets.Scan(&prodName, &qty, &unitprice)
				if odscanerr != nil {
					panic(odscanerr)
				}
				rowCount++
				fmt.Printf("\t\t%-40v %10d %12.2f\n", prodName.String, qty, unitprice)
			}
		}
	}
	fmt.Printf("\nFetched %v rows\n", rowCount)
	tdiff := time.Since(st)
	fmt.Printf("\nTime taken: %v\n", tdiff)
}
