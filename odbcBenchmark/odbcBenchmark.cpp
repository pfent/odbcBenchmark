#include "odbcBenchmark.h"

#include <string>
#include <sql.h>
#include <sqlext.h>
#include <vector>
#include "bench.h"
#include "sqlHelpers.h"

using namespace std;

void fetchAndCheckReturnValue(const SQLHSTMT &statementHandle) {
    auto buffer = std::array<WCHAR, 64>();
    if (SQLBindCol(statementHandle, 1, SQL_C_TCHAR, buffer.data(), buffer.size(), nullptr) == SQL_ERROR) {
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
void doSmallTx(const std::string &connectionString) {
    auto environment = allocateODBC3Environment();
    auto connection = allocateDbConnection(environment.get());
    connectAndPrintConnectionString(connectionString, connection.get());
    checkAndPrintConnection(connection.get());
    auto statementHandle = allocateStatementHandle(connection.get());

    const auto iterations = size_t(1e6);
    auto statement = "SELECT 1;";
    prepareStatement(statementHandle.get(), statement);

    std::cout << "benchmarking " << iterations << " very small transactions" << '\n';

    auto timeTaken = bench([&] {
        for (size_t i = 0; i < iterations; ++i) {
            executeStatement(statementHandle.get());
            checkColumns(statementHandle.get());
            fetchAndCheckReturnValue(statementHandle.get());

            SQLCloseCursor(statementHandle.get());
        }
    });

    cout << " " << iterations / timeTaken << " msg/s\n";

    SQLDisconnect(connection.get());
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
    const auto protocols = {"lpc", "tcp", "np"};

    const auto connectionPrefix = std::string(
            "Driver={SQL Server Native Client 11.0};"
            "Server=");
    const auto connectionSuffix = std::string(
            ":(local);"
            "Database=master;"
            "Trusted_Connection=yes;");

    auto connectionStrings = std::vector<std::string>();

    if (argc == 2) {
        connectionStrings.emplace_back(argv[1]);
    } else {
        std::cout << "usage: odbcBenchmark <connection string>\n"
                  << "now testing all possible connections\n\n";
        for (const auto &protocol : protocols) {
            connectionStrings.emplace_back(connectionPrefix + protocol += connectionSuffix);
        }
    }

    for (const auto &connectionString : connectionStrings) {
        std::cout << "Connecting to " << connectionString << '\n';
        try {
            doSmallTx(connectionString);
        }
        catch (const std::runtime_error &e) {
            std::cout << e.what() << '\n';
        }
        std::cout << '\n';
    }

    std::cout << "done.";
    return 0;
}
