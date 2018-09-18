#include "odbcBenchmark.h"

#include <string>

#ifdef _WIN32
#include <windows.h>
#else
#define GetDesktopWindow() nullptr
#endif

#include <sql.h>
#include <sqlext.h>
#include "bench.h"
#include "sqlHelpers.h"

using namespace std;

void fetchAndCheckReturnValue(const SQLHSTMT &statementHandle) {
    WCHAR buffer[64] = {0};
    if (SQLBindCol(statementHandle, 1, SQL_C_TCHAR, &buffer, 64, nullptr) == SQL_ERROR) {
        throw std::runtime_error("SQLBindCol failed");
    }
    if (SQLFetch(statementHandle) == SQL_ERROR) {
        throw std::runtime_error("SQLFetch failed");
    }

    if (buffer[0] != '1') {
        throw std::runtime_error("unexpected return value from SQL statement");
    }
}

// Do transactions with statements 
// https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-how-to/execute-queries/use-a-statement-odbc
void doTx(std::string connectionString) {
    // TODO: this leaks handles on exception

    auto rawConnectionString = (SQLCHAR *) (connectionString.c_str());
    auto connectionStringLength = connectionString.length();


    SQLHENV environment = nullptr;
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &environment) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }

    if (SQLSetEnvAttr(environment, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0) !=
        SQL_SUCCESS) {
        throw std::runtime_error("SQLSetEnvAttr failed");
    }

    SQLHDBC connection = nullptr;
    if (SQLAllocHandle(SQL_HANDLE_DBC, environment, &connection) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }

    SQLCHAR out[512];
    switch (SQLDriverConnect(connection, GetDesktopWindow(), rawConnectionString, connectionStringLength, out, 512,
                             nullptr, SQL_DRIVER_COMPLETE)) {
        case SQL_SUCCESS:
        case SQL_SUCCESS_WITH_INFO:
            break;
        case SQL_ERROR:
        default:
            throw std::runtime_error("SQLDriverConnect failed, did you enter an invalid connection string?");
    }

    cout << "connected to " << out << '\n';

    checkAndPrintConnection(connection);

    SQLHSTMT statementHandle = nullptr;
    if (SQLAllocHandle(SQL_HANDLE_STMT, connection, &statementHandle) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }

    size_t iterations = 1e6;
    auto statement = "SELECT 1;";
    auto statementLength = strlen(statement);
    if (SQLPrepare(statementHandle, (SQLCHAR *) statement, statementLength) == SQL_ERROR) {
        throw std::runtime_error("SQLPrepare failed");
    }
    auto timeTaken = bench([&] {
        for (size_t i = 0; i < iterations; ++i) {
            executeStatement(statementHandle);
            checkColumns(statementHandle);
            fetchAndCheckReturnValue(statementHandle);

            SQLCloseCursor(statementHandle);
        }
    });

    cout << " " << iterations / timeTaken << " msg/s\n";

    SQLFreeHandle(SQL_HANDLE_STMT, statementHandle);
    SQLDisconnect(connection);
    SQLFreeHandle(SQL_HANDLE_DBC, connection);
    SQLFreeHandle(SQL_HANDLE_ENV, environment);
}

/**
  * ODBC latency benchmark
  * roughly based on http://go.microsoft.com/fwlink/?LinkId=244831
  * and the ODBC API reference: https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference
  */
int main(int argc, char *argv[]) {
    /**
      * Connection method reference:
      * lpc:(local) -> shared memory connection
      * tcp:(local) -> TCP connection on localhost
      * np:(local) -> Named pipe connection
      */
    std::string connectionString =
            "Driver={SQL Server Native Client 11.0};"
            "Server=lpc:(local);"
            "Database=master;"
            "Trusted_Connection=yes;";

    if (argc < 2) {
        cout << "usage: sql <connection string>\n";
    } else {
        connectionString = argv[1];
    }

    cout << "Connecting to " << connectionString << '\n';

    try {
        doTx(connectionString);
    }
    catch (const std::runtime_error &e) {
        cout << e.what() << '\n';
        return -1;
    }

    cout << "done.";
    return 0;
}
