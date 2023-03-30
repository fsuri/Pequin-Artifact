#include <stdlib.h>
using namespace std;
const std::string table_name = "users";
const std::vector<std::pair<std::string, std::string>> column_data_types = {
    std::make_pair("id", "UUID"),
    std::make_pair("city", "STRING"),
    std::make_pair("name", "STRING"),
    std::make_pair("address", "STRING"),
    std::make_pair("credit_card", "STRING"),
    std::make_pair("dl", "STRING")};
const std::vector<uint32_t> pr imary_key_col_idx = {0};
if (FLAGS_replica_idx == 2)
  server->CreateTable(table_name, column_data_types, primary_key_col_idx);
break;