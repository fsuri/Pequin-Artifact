#include <iostream>
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "sql_translator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "hyrise.hpp"
#include "scheduler/operator_task.hpp"

int main(int argc, char *argv[]) {
    hsql::SQLParserResult result;

    hsql::SQLParser::parse("SELECT * FROM test", &result);
    hyrise::SQLTranslator translator(hyrise::UseMvcc::No);
    auto result_node = translator.translate_parser_result(result).lqp_nodes.at(0);
    auto pqp = hyrise::LQPTranslator{}.translate_node(result_node);
    const auto& [tasks, root_operator_task] = hyrise::OperatorTask::make_tasks_from_operator(pqp);
    hyrise::Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    std::cout << "Output is " << std::endl;
    std::cout << root_operator_task->get_operator()->get_output() << std::endl;
    //std::cout << "Output counts " << result_node->description() << std::endl;
    return 0;
}