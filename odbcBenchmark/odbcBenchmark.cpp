#include "odbcBenchmark.h"

#include <string>
#include <vector>
#include <algorithm>
#include <random>
#include "bench.h"
#include "sqlHelpers.h"

using namespace std;

void fetchAndCheckReturnValue(const SQLHSTMT &statementHandle) {
    auto buffer = std::array<WCHAR, 64>();
    bindColumn(statementHandle, 1, buffer);

    fetchBoundColumns(statementHandle);

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

    const auto iterations = size_t(1e6);
    auto statementHandle = allocateStatementHandle(connection.get());
    prepareStatement(statementHandle.get(), "SELECT 1;");

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

void doLargeResultSet(const std::string &connectionString) {
    auto environment = allocateODBC3Environment();
    auto connection = allocateDbConnection(environment.get());
    connectAndPrintConnectionString(connectionString, connection.get());
    checkAndPrintConnection(connection.get());


    const auto results = size_t(1e6);
    const auto recordSize = 1024; // ~ 1GB

    // https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-2017#temporary-tables
    // I.e. Temporary tables starting with a single number sign '#Temp' are automatically dropped when the session ends
    auto createTempTable = allocateStatementHandle(connection.get());
    executeStatement(createTempTable.get(), "CREATE TABLE #Temp (value CHAR(1024) NOT NULL);");

    using Record_t = std::array<WCHAR, recordSize>;

    // 1GB of random characters in records of 1024 chars
    const auto values = [&] {
        auto res = std::vector<Record_t >();
        res.reserve(results);

        auto randomDevice = std::random_device();
        auto distribution = std::uniform_int_distribution<short>('A', 'Z');

        std::generate_n(std::back_inserter(res), results, [&] {
            auto record = Record_t();
            std::generate(record.begin(), record.end(), [&] { return distribution(randomDevice); });
            return record;
        });
        return res;
    }();

    // Fill temp table in batches
    const auto batchSize = 100; // ~100KB hopefully works with SQLExecuteDirect
    static_assert(results % batchSize == 0);

    for (int i = 0; i < results; i += batchSize) {
        auto statement = std::string("INSERT INTO #MyTempTable VALUES (");
        for (int j = i; j < i + batchSize; j++) {
            statement += "'" + std::string(values[j].begin(), values[j].end()) + "'";
            if (j < i + batchSize - 1) {
                statement += ',';
            }
        }
        statement += ");";
        auto insertTempTable = allocateStatementHandle(connection.get());
        executeStatement(insertTempTable.get(), statement.c_str());
    }

    const auto resultSizeMB = static_cast<double>(results) * sizeof(Record_t) / 1024 / 1024;
    std::cout << "benchmarking " << resultSizeMB << "MB data transfer" << '\n';
    auto selectFromTempTable = allocateStatementHandle(connection.get());
    prepareStatement(selectFromTempTable.get(), "SELECT value FROM #Temp");

    auto timeTaken = bench([&] {
        executeStatement(selectFromTempTable.get());
        checkColumns(selectFromTempTable.get());

        auto record = Record_t();
        bindColumn(selectFromTempTable.get(), 1, record);

        for (size_t i = 0; i < results; ++i) {
            fetchBoundColumns(selectFromTempTable.get());

            if(record != values[i]) {
                throw std::runtime_error("unexpected return value from SQL statement");
            }
        }
        SQLCloseCursor(selectFromTempTable.get());
    });

    cout << " " << resultSizeMB / timeTaken << " MB/s\n";

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
