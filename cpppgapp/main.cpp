#include <iostream>
#include <iomanip>
#include <string>
#include "postgresql/libpq-fe.h"
#include <locale>

using namespace std;

struct Result {
    PGresult *res;
    int numCols;
    int numRows;

    char *getValue(int rowNum, int colNum) {
        return PQgetvalue(res, rowNum, colNum);
    }

    void dumpResult(char colSep=' ') {
        for (int r=0; r < numRows; r++) {
            for (int c=0; c < numCols; c++) {
                cout << getValue(r,c) << " ";
                if (c < numCols-1)
                    cout << colSep;
                cout << " ";
            }
            cout << endl;
        }
    }
};

class PgDB
{
    PGconn *conn;
public:
    PgDB(const char *dbname, const char *user, const char *pwd) {
        string conninfo;
        conninfo.append("user=")
        .append(user)
        .append(" password=")
        .append(pwd)
        .append(" port=5432 host=localhost dbname=")
        .append(dbname);
        cout << "conn-info: " << conninfo << endl;
        conn = PQconnectdb(conninfo.c_str());
        if (PQstatus(conn) != CONNECTION_OK) {
            cout << "connection to DB failed" << PQerrorMessage(conn) << endl;
            PQfinish(conn);
            exit(1);
        }
        cout << "Successfully connected to Db" << endl;
    }

    Result getResult(const char *qry) {
        Result result;
        result.res = PQexec(conn, qry);
        result.numCols = PQnfields(result.res);
        result.numRows = PQntuples(result.res);
        return result;
    }

};

int main() {
    cout << "Application to access Postgreql in C++" << endl;

    int totalRows = 0;

    PgDB db("northwind", "postgres", "toor");

    cout << endl << "CATEGORIES" << endl << endl;

    Result catRes = db.getResult("Select \"CategoryID\", \"CategoryName\" From categories");
    catRes.dumpResult('|');
    cout << "Total category Rows # " << catRes.numRows << endl;
    totalRows += catRes.numRows;

    cout << endl << "PRODUCTS" << endl << endl;

    Result prodRes = db.getResult("Select \"ProductID\", \"ProductName\" From products");
    prodRes.dumpResult('|');
    cout << "Total products Rows # " << prodRes.numRows << endl;
    totalRows += prodRes.numRows;

    cout << endl << "CUSTOMERS" << endl << endl;

    Result custRes = db.getResult("Select \"CustomerID\", \"CompanyName\" From customers");
    if (!custRes.numRows) {
        cout << endl << "No customers fetched!" << endl << endl;
    } else {
        custRes.dumpResult('|');
        cout << "Total customers Rows # " << custRes.numRows << endl;
        totalRows += custRes.numRows;
    }

    cout << endl << "ORDERS" << endl << endl;

    Result ordRes = db.getResult("Select \"OrderID\", to_char(\"OrderDate\",'dd-mm-yyyy') \"OrderDate\", to_char(\"ShippedDate\",'dd-mm-yyyy') \"ShippedDate\" From orders");
    ordRes.dumpResult('|');
    cout << "Total orders Rows # " << ordRes.numRows << endl;
    totalRows += ordRes.numRows;


    cout << endl << "ORDER DETAILS" << endl << endl;

    Result odetRes = db.getResult("Select od.\"OrderID\", p.\"ProductName\", od.\"Quantity\", od.\"UnitPrice\" From order_details od Join products p on p.\"ProductID\" = od.\"ProductID\"");
    odetRes.dumpResult('|');
    cout << "Total order details Rows # " << odetRes.numRows << endl;
    totalRows += odetRes.numRows;

    cout << endl << "Fetched " << totalRows << endl;

    return 0;
}