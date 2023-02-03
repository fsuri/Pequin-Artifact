#include <iostream>
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "sql_translator.hpp"

int main(int argc, char *argv[]) {
    hsql::SQLParserResult result;

    hsql::SQLParser::parse("SELECT * FROM test", &result);
    hyrise::SQLTranslator translator(hyrise::UseMvcc::No);
    auto result_node = translator.translate_parser_result(result).lqp_nodes.at(0);
    std::cout << "Output counts " << result_node->description() << std::endl;
    return 0;
}