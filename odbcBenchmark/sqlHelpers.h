#pragma once

#include <array>
#include <cstring>
#include <iostream>
#include <memory>
#include <type_traits>

#ifdef _WIN32
#include <windows.h>
#else
#define GetDesktopWindow() nullptr
#endif

#include <sql.h>
#include <sqlext.h>

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

using StatementHandle = std::unique_ptr<std::remove_pointer_t<SQLHSTMT>, decltype(&freeStatementHandle)>;

auto allocateStatementHandle(SQLHDBC connection) {
   auto statementHandle = SQLHSTMT();
   if (SQLAllocHandle(SQL_HANDLE_STMT, connection, &statementHandle) != SQL_SUCCESS) {
      throw std::runtime_error("SQLAllocHandle failed");
   }
   return StatementHandle(statementHandle, &freeStatementHandle);
}

void prepareStatement(SQLHSTMT statementHandle, const char* statement) {
   const auto statementLength = SQLINTEGER(strlen(statement));
   if (SQLPrepare(statementHandle, (SQLCHAR*) statement, statementLength) == SQL_ERROR) {
      throw std::runtime_error("SQLPrepare failed");
   }
}

void handleError(SQLRETURN res, SQLSMALLINT handleType, SQLHANDLE handle) {
   auto error = std::string("Return code: " + std::to_string(res));
   SQLLEN numRecs = 0;
   SQLGetDiagField(handleType, handle, 0, SQL_DIAG_NUMBER, &numRecs, 0, nullptr);

   SQLINTEGER NativeError;
   SQLCHAR SqlState[6];
   SQLCHAR Msg[SQL_MAX_MESSAGE_LENGTH];
   SQLSMALLINT MsgLen;
   for (SQLSMALLINT i = 1;
        i <= numRecs &&
        SQLGetDiagRec(handleType, handle, i, SqlState, &NativeError, Msg, sizeof(Msg), &MsgLen) !=
        SQL_NO_DATA;
        ++i) {
      error += std::string("\nSqlState: ") + std::string(std::begin(SqlState), std::end(SqlState)) +
               std::string("\nNativeError: ") + std::to_string(NativeError) +
               std::string("\nMessage: ") + std::string(Msg, &Msg[MsgLen]);
   }
   throw std::runtime_error("SQLDriverConnect failed, did you enter an invalid connection string?" + error);
}

void connectAndPrintConnectionString(const std::string &connectionString, SQLHDBC connection) {
   auto rawConnectionString = (SQLCHAR*) (connectionString.c_str());
   const auto connectionStringLength = SQLSMALLINT(connectionString.length());
   auto out = std::array<SQLCHAR, 512>();
   const auto res = SQLDriverConnect(connection, GetDesktopWindow(), rawConnectionString, connectionStringLength,
                                     out.data(),
                                     SQLSMALLINT(out.size()), nullptr, SQL_DRIVER_COMPLETE);
   switch (res) {
      case SQL_SUCCESS:
      case SQL_SUCCESS_WITH_INFO:
         break;
      case SQL_ERROR:
      default:
         handleError(res, SQL_HANDLE_DBC, connection);
   }

   std::cout << "connected to " << std::string(out.begin(), out.end()) << '\n';
}

void connect(const std::string &serverName, const std::string &userName, const std::string &password,
             SQLHDBC connection) {
   auto rawServerName = (SQLCHAR*) (serverName.c_str());
   const auto serverNameLength = SQLSMALLINT(serverName.length());
   auto rawUserName = (SQLCHAR*) (userName.c_str());
   const auto userNameLength = SQLSMALLINT(userName.length());
   auto rawPassword = (SQLCHAR*) (password.c_str());
   const auto passwordLength = SQLSMALLINT(password.length());

   switch (SQLConnect(connection, rawServerName, serverNameLength, rawUserName, userNameLength, rawPassword,
                      passwordLength)) {
      case SQL_SUCCESS:
      case SQL_SUCCESS_WITH_INFO:
         break;
      case SQL_ERROR:
      default:
         throw std::runtime_error("SQLDriverConnect failed, did you enter an invalid connection string?");
   }
}

void executeStatement(const SQLHSTMT &statementHandle) {
   if (SQLExecute(statementHandle) == SQL_ERROR) {
      throw std::runtime_error("SQLExecute failed");
   }
}

void executeStatement(const SQLHSTMT &statementHandle, const char* statement) {
   const auto statementLength = SQLINTEGER(strlen(statement));
   if (SQLExecDirect(statementHandle, (SQLCHAR*) statement, statementLength) == SQL_ERROR) {
      throw std::runtime_error(std::string("SQLExecDirect failed: ") + statement);
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

void bindKeyParam(const SQLHSTMT &statementHandle, uint32_t &key) {
   SQLBindParameter(statementHandle, 1, SQL_PARAM_INPUT, SQL_C_ULONG,
                    SQL_INTEGER, 10, 0, &key, 1, nullptr);
}

template<typename bufferType, size_t bufferSize>
void
bindColumn(const SQLHSTMT &statementHandle, SQLUSMALLINT columnNumber, std::array<bufferType, bufferSize> &buffer) {
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

   if (SQLExecDirect(statementHandle, (SQLCHAR*) connectionTest, length) != SQL_SUCCESS) {
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
