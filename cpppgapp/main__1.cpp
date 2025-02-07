#include <iostream>
// #include <pqxx/pqxx>
#include <iomanip>
#include <string>
#include <map>

#include "postgresql/libpq-fe.h"

using namespace std;


int main() {
    const char *conninfo;
    // pqxx::internal::pq::PGconn *conn;
    PGconn      *conn;
    PGresult    *res;

    long totalRowsFetched = 0;

    // conninfo = "user=postgres password=toor port=5432 host=localhost dbname=northwind target_session_attrs=read-write";
    conninfo = "user=postgres password=toor port=5432 host=localhost dbname=northwind";

    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        cout << "connection to DB failed" << PQerrorMessage(conn) << endl;
        PQfinish(conn);
        exit(1);
    }

    cout << "Application to access Postgreql in C++" << endl
        << "connected to db" << endl;

    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        cout << "BEGIN command failed " << PQerrorMessage(conn);
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
    
    PGresult *catRes = PQexec(conn, "Select \"CategoryID\", \"CategoryName\", \"Description\" From categories");
    
    int catColCount = PQnfields(catRes);
    int catRowCount = PQntuples(catRes);

    totalRowsFetched += catRowCount;

    cout << "column count " << catColCount << endl;
    cout << "rows fetched " << catRowCount << endl;

    for (int r=0; r < catRowCount; r++) {
        cout << right << setw(5) << PQgetvalue(res, r, 0) << " | "
            << left << setw(30) << PQgetvalue(res, r, 1) << " | "
            << left << PQgetvalue(res, r, 2)
            << endl;
    }
    
    PGresult *prodRes = PQexec(conn, "Select \"ProductID\", \"ProductName\", \"UnitPrice\", \"ReorderLevel\" From products");
    
    int prodColCount = PQnfields(prodRes);
    int prodRowCount = PQntuples(prodRes);
    totalRowsFetched == prodRowCount;

    cout << endl << "PRODUCTS" << endl;

    cout << "column count " << prodColCount << endl;
    cout << "rows fetched " << prodRowCount << endl;

    for (int r=0; r < prodRowCount; r++) {
        cout << right << setw(5) << PQgetvalue(res, r, 0) << " | "
            << left << setw(40) << fixed << string(PQgetvalue(res, r, 1)) << " | "
            << right << setw(12) << setprecision(2) << fixed << stof(PQgetvalue(res, r, 2))
            << right << setw(10) << stoi(PQgetvalue(res, r, 3))
            << endl;
    }

    PGresult *custRes = PQexec(conn, "Select \"CustomerID\", \"CompanyName\", \"City\", \"Phone\" From customers");
    
    int custColCount = PQnfields(custRes);
    int custRowCount = PQntuples(custRes);

    totalRowsFetched += custRowCount;
    
    cout << endl << "CUSTOMERS" << endl;

    cout << "column count " << custColCount << endl;
    cout << "rows fetched " << custRowCount << endl;

    for (int r=0; r < custRowCount; r++) {
        cout << right << setw(5) << PQgetvalue(custRes, r, 0) << " | "
            << left << setw(40) << fixed << string(PQgetvalue(custRes, r, 1)) << " | "
            << left << setw(20) << fixed << PQgetvalue(custRes, r, 2)
            << left << setw(20) << fixed << PQgetvalue(custRes, r, 3)
            << endl;
    }

    PGresult *ordres = PQexec(conn, "Select \"OrderID\", to_char(\"OrderDate\",'dd-mm-yyyy') \"OrderDate\", to_char(\"ShippedDate\",'dd-mm-yyyy') \"ShippedDate\" From orders");
    
    int ordColCount = PQnfields(ordres);
    int ordRowCount = PQntuples(ordres);
    totalRowsFetched += ordRowCount;
    
    cout << endl << "ORDERS" << endl;

    cout << "column count " << ordColCount << endl;
    cout << "rows fetched " << ordRowCount << endl;

    for (int r=0; r < ordRowCount; r++) {
        cout << right << setw(8) << PQgetvalue(res, r, 0) << " | "
            << left << setw(20) << fixed << PQgetvalue(res, r, 1) << " | "
            << left << setw(20) << fixed << PQgetvalue(res, r, 2)
            << endl;
    }


    PGresult *odetRes = PQexec(conn, "Select od.\"OrderID\", p.\"ProductName\", od.\"Quantity\", od.\"UnitPrice\" From order_details od Join products p on p.\"ProductID\" = od.\"ProductID\"");
    
    int odColCount = PQnfields(odetRes);
    int odRowCount = PQntuples(odetRes);
    totalRowsFetched += odRowCount;
    
    cout << endl << "ORDER DETAILS" << endl;

    cout << "column count " << odColCount << endl;
    cout << "rows fetched " << odRowCount << endl;

    for (int r=0; r < odRowCount; r++) {
        cout << right << setw(8) << PQgetvalue(odetRes, r, 0) << " | "
            << left << setw(40) << fixed << PQgetvalue(odetRes, r, 1) << " | "
            << right << setw(10) << fixed << stoi(PQgetvalue(odetRes, r, 2))
            << right << setw(12) << fixed << setprecision(2) << stof(PQgetvalue(odetRes, r, 3))
            << endl;
    }

    cout << endl << "Customer wise Order details" 
        << endl << "---------------------------" << endl;
        

    cout << "cust column count " << custColCount << endl;
    cout << "cust rows fetched " << custRowCount << endl;

    totalRowsFetched += custRowCount;

    // orders prepared statement
    const char *ORDSTM = "Select \"OrderID\", to_char(\"OrderDate\",'dd-mm-yyyy') \"OrderDate\", to_char(\"ShippedDate\",'dd-mm-yyyy') \"ShippedDate\" From orders Where \"CustomerID\" = $1";

    // order details prepared statement
    const char *ORDETSTM = "Select od.\"OrderID\", p.\"ProductName\", od.\"Quantity\", od.\"UnitPrice\" From order_details od Join products p on p.\"ProductID\" = od.\"ProductID\" Where \"OrderID\" = $1";

    const char *paramValues[1];
    const char *paramValuesOdet[1];

    char *custid;
    char *orderid;

    char qstr[10];
    char qstrOdet[10];

    for (int r=0; r < custRowCount; r++) {
        custid = PQgetvalue(custRes, r, 0);
        cout << endl << right << setw(5) << custid << " | "
            << left << setw(40) << fixed << string(PQgetvalue(custRes, r, 1)) << " | "
            << left << setw(20) << fixed << PQgetvalue(custRes, r, 2) << " | "
            << left << setw(20) << fixed << PQgetvalue(custRes, r, 3)
            << endl;

        snprintf(qstr, 10, "%s", custid);
        paramValues[0] = qstr;

        // cout << endl << "param values (qstr) : [" << paramValues[0] << "] | query string: " << ORDSTM << endl;
        PGresult *innerOres = PQexecParams(conn, ORDSTM, 1, NULL, paramValues, NULL, NULL, 0);

        int innerOrdersCount = PQntuples(innerOres);
        totalRowsFetched += innerOrdersCount;

        cout << endl << "\t-- ORDERS --- for " << paramValues[0] << " | #" << innerOrdersCount << " fetched\n\n";

        for (int orderNo=0; orderNo < innerOrdersCount; orderNo++) {
            orderid = PQgetvalue(innerOres, orderNo, 0);
            cout << right << setw(8) << orderid << " | "
                << left << setw(20) << fixed << PQgetvalue(innerOres, orderNo, 1) << " | "
                << left << setw(20) << fixed << PQgetvalue(innerOres, orderNo, 2)
                << endl;

            snprintf(qstrOdet, 10, "%s", orderid);
            paramValuesOdet[0] = qstrOdet;

            // cout << endl << "param values (qstr) : [" << paramValues[0] << "] | query string: " << ORDSTM << endl;
            PGresult *innerOdetRes = PQexecParams(conn, ORDETSTM, 1, NULL, paramValuesOdet, NULL, NULL, 0);

            int innerOrderDetCount = PQntuples(innerOdetRes);
            totalRowsFetched += innerOrderDetCount;
            
            cout << endl << "\t** Order Details" << endl;
            
            for (int odn=0; odn < innerOrderDetCount; odn++) {
                cout << "\t\t**" << right << setw(8) << PQgetvalue(innerOdetRes, odn, 0) << " | "
                    << left << setw(40) << fixed << PQgetvalue(innerOdetRes, odn, 1) << " | "
                    << left << setw(10) << fixed << PQgetvalue(innerOdetRes, odn, 2) << " | "
                    << left << setw(12) << fixed << setprecision(2) << stof(PQgetvalue(innerOdetRes, odn, 3))
                    << endl;
            }            
        }
    }


    cout << endl << "Total rows fetched = " << totalRowsFetched << endl;
    
    PQclear(res);

    return 0;
}