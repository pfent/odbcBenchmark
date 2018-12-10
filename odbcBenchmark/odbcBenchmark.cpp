#include <vector>
#include "benchmarks.h"

using namespace std;

/**
  * ODBC latency benchmark
  * roughly based on http://go.microsoft.com/fwlink/?LinkId=244831
  * and the ODBC API reference: https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference
  */
int main(int argc, char* argv[]) {
   /**
     * Connection method reference:
     * lpc:(local) -> shared memory connection
     * tcp:(local) -> TCP connection on localhost
     * np:(local) -> Named pipe connection
     */
   const auto protocols = {"lpc", "tcp", "np"};

   /// Available drivers can be seen in odbcinst.ini
   /// Localtion on Linux: /etc/odbcinst.ini
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
         auto environment = allocateODBC3Environment();
         auto connection = allocateDbConnection(environment.get());
         connectAndPrintConnectionString(connectionString, connection.get());
         checkAndPrintConnection(connection.get());

         doSmallTx(connection.get());
         doLargeResultSet(connection.get());
         doInternalSmallTx(connection.get());
         SQLDisconnect(connection.get());
      }
      catch (const std::runtime_error &e) {
         std::cout << e.what() << '\n';
      }

      std::cout << '\n';
   }

   std::cout << "done.";
   return 0;
}
