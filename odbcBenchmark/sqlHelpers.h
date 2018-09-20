#ifndef ODBCBENCHMARK_SQLHELPERS_H
#define ODBCBENCHMARK_SQLHELPERS_H

#include <array>
#include <cstring>
#include <iostream>
#include <memory>
#include <type_traits>
#include <sql.h>
#include <sqlext.h>

#ifdef _WIN32
#include <windows.h>
#else
#define GetDesktopWindow() nullptr
#endif

void freeODBC3Environment(SQLHENV environment) { SQLFreeHandle(SQL_HANDLE_ENV, environment); }

auto allocateODBC3Environment() {
    auto environment = SQLHENV();
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &environment) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }
    if (SQLSetEnvAttr(environment, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0) !=
        SQL_SUCCESS) {
        throw std::runtime_error("SQLSetEnvAttr failed");
    }
    return std::unique_ptr<std::remove_pointer_t<SQLHENV>, decltype(&freeODBC3Environment)>
            (environment, &freeODBC3Environment);
}

void freeDbConnection(SQLHDBC connection) { SQLFreeHandle(SQL_HANDLE_DBC, connection); }

auto allocateDbConnection(SQLHENV environment) {
    auto connection = SQLHDBC();
    if (SQLAllocHandle(SQL_HANDLE_DBC, environment, &connection) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }
    return std::unique_ptr<std::remove_pointer_t<SQLHDBC>, decltype(&freeDbConnection)>(connection, &freeDbConnection);
}

void freeStatementHandle(SQLHSTMT statementHandle) { SQLFreeHandle(SQL_HANDLE_STMT, statementHandle); }

auto allocateStatementHandle(SQLHDBC connection) {
    auto statementHandle = SQLHSTMT();
    if (SQLAllocHandle(SQL_HANDLE_STMT, connection, &statementHandle) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }
    return std::unique_ptr<std::remove_pointer_t<SQLHSTMT>, decltype(&freeStatementHandle)>(statementHandle, &freeStatementHandle);
}

void prepareStatement(SQLHSTMT statementHandle, const char *statement) {
    const auto statementLength = SQLINTEGER(strlen(statement));
    if (SQLPrepare(statementHandle, (SQLCHAR *) statement, statementLength) == SQL_ERROR) {
        throw std::runtime_error("SQLPrepare failed");
    }
}

void connectAndPrintConnectionString(const std::string &connectionString, SQLHDBC connection) {
    auto rawConnectionString = (SQLCHAR *) (connectionString.c_str());
    const auto connectionStringLength = SQLSMALLINT(connectionString.length());
    auto out = std::array<SQLCHAR, 512>();
    switch (SQLDriverConnect(connection, GetDesktopWindow(), rawConnectionString, connectionStringLength, out.data(),
                             out.size(), nullptr, SQL_DRIVER_COMPLETE)) {
        case SQL_SUCCESS:
        case SQL_SUCCESS_WITH_INFO:
            break;
        case SQL_ERROR:
        default:
            throw std::runtime_error("SQLDriverConnect failed, did you enter an invalid connection string?");
    }

    std::cout << "connected to " << std::string(out.begin(), out.end()) << '\n';
}

void executeStatement(const SQLHSTMT &statementHandle) {
    if (SQLExecute(statementHandle) == SQL_ERROR) {
        throw std::runtime_error("SQLExecute failed");
    }
}

void executeStatement(const SQLHSTMT& statementHandle, const char* statement) {
    const auto statementLength = SQLINTEGER(strlen(statement));
    if(SQLExecDirect(statementHandle, (SQLCHAR*) statement, statementLength) == SQL_ERROR) {
        throw std::runtime_error("SQLExecDirect failed");
    }
}

void checkColumns(const SQLHSTMT &statementHandle, SQLSMALLINT numCols = 1) {
    auto cols = SQLSMALLINT();
    if (SQLNumResultCols(statementHandle, &cols) == SQL_ERROR) {
        throw std::runtime_error("SQLNumResultCols failed");
    }
    if (cols != numCols) {
        throw std::runtime_error("unexpected number of columns");
    }
}

template <size_t bufferSize>
void bindColumn(const SQLHSTMT &statementHandle, SQLUSMALLINT columnNumber, std::array<WCHAR, bufferSize> buffer) {
    if (SQLBindCol(statementHandle, columnNumber, SQL_C_TCHAR, buffer.data(), buffer.size(), nullptr) == SQL_ERROR) {
        throw std::runtime_error("SQLBindCol failed");
    }
}

void fetchBoundColumns(const SQLHSTMT &statementHandle) {
    if (SQLFetch(statementHandle) == SQL_ERROR) {
        throw std::runtime_error("SQLFetch failed");
    }
}

void checkAndPrintConnection(SQLHDBC connection) {
    auto connectionTest = "select net_transport from sys.dm_exec_connections where session_id = @@SPID;";
    const auto length = SQLINTEGER(::strlen(connectionTest));

    auto statementHandle = SQLHSTMT();
    if (SQLAllocHandle(SQL_HANDLE_STMT, connection, &statementHandle) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }

    if (SQLExecDirect(statementHandle, (SQLCHAR *) connectionTest, length) != SQL_SUCCESS) {
        throw std::runtime_error("SQLExecDirect failed");
    }

    auto buffer = std::array<SQLCHAR, 64>();
    if (SQLBindCol(statementHandle, 1, SQL_CHAR, buffer.data(), buffer.size(), nullptr) != SQL_SUCCESS) {
        throw std::runtime_error("SQLBindCol failed");
    }
    if (SQLFetch(statementHandle) != SQL_SUCCESS) {
        throw std::runtime_error("SQLFetch failed");
    }

    std::cout << "Connected via: ";
    for (auto c : buffer) {
        std::cout << c;
    }
    std::cout << '\n';
}

#endif //ODBCBENCHMARK_SQLHELPERS_H
