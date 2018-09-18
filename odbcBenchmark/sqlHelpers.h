#ifndef ODBCBENCHMARK_SQLHELPERS_H
#define ODBCBENCHMARK_SQLHELPERS_H

#include <array>
#include <cstring>
#include <iostream>
#include <sql.h>
#include <sqlext.h>

SQLHENV allocateODBC3Environment() {
    auto environment = SQLHENV();
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &environment) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }
    if (SQLSetEnvAttr(environment, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0) !=
        SQL_SUCCESS) {
        throw std::runtime_error("SQLSetEnvAttr failed");
    }
    return environment;
}

SQLHDBC allocateDbConnection(SQLHENV environment) {
    auto connection = SQLHDBC();
    if (SQLAllocHandle(SQL_HANDLE_DBC, environment, &connection) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }
    return connection;
}

SQLHSTMT allocateStatementHandle(SQLHDBC connection) {
    auto statementHandle = SQLHSTMT();
    if (SQLAllocHandle(SQL_HANDLE_STMT, connection, &statementHandle) != SQL_SUCCESS) {
        throw std::runtime_error("SQLAllocHandle failed");
    }
    return statementHandle;
}

void prepareStatement(SQLHSTMT statementHandle, const char* statement) {
    const auto statementLength = SQLINTEGER(strlen(statement));
    if (SQLPrepare(statementHandle, (SQLCHAR *) statement, statementLength) == SQL_ERROR) {
        throw std::runtime_error("SQLPrepare failed");
    }
}

void executeStatement(const SQLHSTMT &statementHandle) {
    if (SQLExecute(statementHandle) == SQL_ERROR) {
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

void checkAndPrintConnection(SQLHDBC &connection) {
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
