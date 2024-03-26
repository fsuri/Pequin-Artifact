const std::string table_name = "students";
const std::vector<std::pair<std::string, std::string>> column_data_types = {
    std::make_pair("rollNumber", "INT"), std::make_pair("name", "VARCHAR(30)"),
    std::make_pair("class", "VARCHAR(30)"),
    std::make_pair("section", "VARCHAR(1)"),
    std::make_pair("mobile", "VARCHAR(10)")};
const std::vector<uint32_t> primary_key_col_idx = {0, 4};
std::vector<std::string> values = {"1", "BEN", "FOURTH", "B", "4204206969"};
if (FLAGS_replica_idx == 2) {
  server->CreateTable(table_name, column_data_types, primary_key_col_idx);
  server->LoadTableRow(table_name, column_data_types, values,
                       primary_key_col_idx);
}
break;