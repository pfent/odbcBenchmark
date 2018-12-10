#include <vector>
#include "benchmarks.h"

using namespace std;

/**
  * ODBC benchmark with simpler connection string interface
  */
int main(int argc, char* argv[]) {
   if (argc < 4) {
      std::cout << "usage: odbcBenchmarkSQLConnect <host> <user> <password>";
      return -1;
   }
   auto serverName = argv[1];
   auto userName = argv[2];
   auto password = argv[3];

   std::cout << "Connecting...\n";
   try {
      auto environment = allocateODBC3Environment();
      auto connection = allocateDbConnection(environment.get());
      connect(serverName, userName, password, connection.get());
      checkAndPrintConnection(connection.get());

      prepareYcsb(connection.get());
      doSmallTx(connection.get());
      doLargeResultSet(connection.get());
      doInternalSmallTx(connection.get());
      SQLDisconnect(connection.get());
   }
   catch (const std::runtime_error &e) {
      std::cout << e.what() << '\n';
   }

   std::cout << "done.";
   return 0;
}
