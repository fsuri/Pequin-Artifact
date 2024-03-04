// Parser for the custom engine
// Must do syntax checking and generate a query tree.
extern "C" {
#include "pg_query.h"
}
#include <stdio.h>

#include <iostream>
#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include <tuple>

int main(int argc, char *argv[]) {
  auto ctx = pg_query_parse_init();
  // auto result = pg_query_parse("SELECT col1, COUNT(*) from tableA where col2
  // >= 3 limit 20;");
  auto result = pg_query_parse(
      "INSERT INTO tableA VALUES (1, \"abracadabra\", 30), (2, 4, 5);");

  if (result.error) {
    return 1;
  }

  auto tree_json = pg_parse_tree_json(result.tree);
  // printf("%s\n", tree_json);
  json query = json::parse(tree_json);
  // printf("%s\n", query.dump(4).c_str())
  // query.contains("SelectStmt");

  // std::cout
  //     << query[0]["InsertStmt"]["selectStmt"]["SelectStmt"]["valuesLists"][0]
  //            .dump(2)
  //     << std::endl;

  using Write = std::tuple<int, int, std::string>;

  std::vector<Write> rows_contents;
  auto rows = query[0]["InsertStmt"]["selectStmt"]["SelectStmt"]["valuesLists"];
  for (auto row : rows) {
    Write row_tuple = std::make_tuple(
        row[0]["A_Const"]["val"]["Integer"]["ival"], 0, row.dump());
    rows_contents.push_back(row_tuple);
  }

  for (auto& el: rows_contents) {
    std::cout << std::get<0>(el) << std::get<1>(el) << std::get<2>(el) << std::endl;
  }
  // std::cout << query[0]["labradoodle"]["yee"] << std::endl;

  pg_parse_tree_json_free(tree_json);
  pg_query_parse_finish(ctx);
  pg_query_free_parse_result(result);
  return 0;
}
