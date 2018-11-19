#include "odbcBenchmark.h"

#include <vector>
#include <algorithm>
#include <random>
#include "bench.h"
#include "sqlHelpers.h"
#include "ycsb.h"

using namespace std;

static auto db = YcsbDatabase();

template<typename E>
void fetchAndCheckReturnValue(const SQLHSTMT &statementHandle, const E *expected) {
    auto buffer = std::array<char, ycsb_field_length>();
    bindColumn<char>(statementHandle, 1, buffer);

    fetchBoundColumns(statementHandle);

    if (!std::equal(buffer.begin(), buffer.end(), expected)) {
        throw std::runtime_error("unexpected return value from SQL statement");
    }
}

void prepareYcsb(SQLHDBC connection) {
    auto create = std::string("CREATE TABLE #Ycsb ( key INTEGER PRIMARY KEY NOT NULL, ");
    for (size_t i = 1; i < ycsb_field_count; ++i) {
        create += "v" + std::to_string(i) + " NCHAR(" + std::to_string(ycsb_field_length) + ") NOT NULL, ";
    }
    create += std::to_string(ycsb_field_count) + " NCHAR(" + std::to_string(ycsb_field_length) + ") NOT NULL";
    create += ");";

    for (auto&[key, value] : db.database) {
        auto statement = std::string("INSERT INTO #Ycsb VALUES ");
        statement += "(" + std::to_string(key) + ", ";
        for (auto &v : value.rows) {
            statement += v.data();
            statement += ", ";
        }
        statement.resize(statement.length() - 2); // remove last comma
        statement += ")";
        statement += ";";
        auto insertTempTable = allocateStatementHandle(connection);
        executeStatement(insertTempTable.get(), statement.c_str());
    }
}

// Do transactions with statements 
// https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-how-to/execute-queries/use-a-statement-odbc
void doSmallTx(const std::string &connectionString) {
    auto environment = allocateODBC3Environment();
    auto connection = allocateDbConnection(environment.get());
    connectAndPrintConnectionString(connectionString, connection.get());
    checkAndPrintConnection(connection.get());
    prepareYcsb(connection.get());

    auto columnStatements = std::vector<StatementHandle>();
    for (size_t i = 1; i < ycsb_field_count + 1; ++i) {
        columnStatements.push_back(allocateStatementHandle(connection.get()));
        auto statement = std::string("SELECT v") + std::to_string(i) + " FROM #Ycsb WHERE key=?;";
        prepareStatement(columnStatements.back().get(), statement.c_str());
    }

    auto rand = Random32();
    const auto lookupKeys = generateZipfLookupKeys(ycsb_tx_count);

    std::cout << "benchmarking " << lookupKeys.size() << " small transactions" << '\n';

    auto timeTaken = bench([&] {
        for (auto lookupKey: lookupKeys) {
            auto which = rand.next();

            bindKeyParam(columnStatements[which].get(), lookupKey);
            executeStatement(columnStatements[which].get());
            checkColumns(columnStatements[which].get());

            auto result = std::array<wchar_t, ycsb_field_length>();
            db.lookup(lookupKey, which, result.begin());
            fetchAndCheckReturnValue(columnStatements[which].get(), result.begin());

            SQLCloseCursor(columnStatements[which].get());
        }
    });

    cout << " " << lookupKeys.size() / timeTaken << " msg/s\n";

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

    using Record_t = std::array<wchar_t, recordSize>;

    // 1GB of random characters in records of 1024 chars
    const auto values = [&] {
        auto res = std::vector<Record_t>();
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
    const auto batchSize = 1000; // ~1MB hopefully works with SQLExecuteDirect
    static_assert(results % batchSize == 0);

    for (size_t i = 0; i < results; i += batchSize) {
        auto statement = std::string("INSERT INTO #Temp VALUES ");
        statement.reserve(batchSize * recordSize);
        for (size_t j = i; j < i + batchSize; j++) {
            statement += "('" + std::string(values[j].begin(), values[j].end()) + "')";
            if (j < i + batchSize - 1) {
                statement += ',';
            }
        }
        statement += ";";
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
        bindColumn<wchar_t>(selectFromTempTable.get(), 1, record);

        for (size_t i = 0; i < results; ++i) {
            fetchBoundColumns(selectFromTempTable.get());
        }
        SQLCloseCursor(selectFromTempTable.get());
    });

    cout << " " << resultSizeMB / timeTaken << " MB/s\n";

    createTempTable.reset();
    selectFromTempTable.reset();
    SQLDisconnect(connection.get());
}

void doInternalSmallTx(const std::string &connectionString) {
    auto environment = allocateODBC3Environment();
    auto connection = allocateDbConnection(environment.get());
    connectAndPrintConnectionString(connectionString, connection.get());
    checkAndPrintConnection(connection.get());

    const auto iterations = size_t(1e6);
    auto statementHandle = allocateStatementHandle(connection.get());
    const auto statement = std::string()
                           + "DECLARE @i int = 0;\n"
                           + "WHILE @i < " + to_string(iterations)
                           + "BEGIN\n"
                           + "    SELECT 1;\n"
                           + "    SET @i = @i + 1\n"
                           + "END";
    prepareStatement(statementHandle.get(), statement.c_str());

    std::cout << "benchmarking " << iterations << " very small internal transactions" << '\n';

    const auto averaging = size_t(1e2);
    auto timeTaken = bench([&] {
        for (size_t i = 0; i < averaging; ++i) {
            executeStatement(statementHandle.get());

            auto buffer = std::array<wchar_t, 64>();
            bindColumn<wchar_t>(statementHandle.get(), 1, buffer);

            for (size_t j = 0; j < iterations; ++j) {
                fetchBoundColumns(statementHandle.get());
                if (buffer[0] != '1') {
                    throw std::runtime_error("unexpected return value from SQL statement");
                }
            }

            SQLCloseCursor(statementHandle.get());
        }
    });

    cout << " " << iterations / (timeTaken / averaging) << " msg/s\n";

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
            //"Driver={SQL Server Native Client 11.0};"
            "Driver={ODBC Driver 13 for SQL Server};"
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
            doLargeResultSet(connectionString);
            doInternalSmallTx(connectionString);
        }
        catch (const std::runtime_error &e) {
            std::cout << e.what() << '\n';
        }
        std::cout << '\n';
    }

    std::cout << "done.";
    return 0;
}
