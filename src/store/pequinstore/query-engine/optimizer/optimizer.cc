//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer.cpp
//
// Identification: src/optimizer/optimizer.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "../optimizer/optimizer.h"

#include "../catalog/column_catalog.h"
#include "../catalog/manager.h"
#include "../catalog/table_catalog.h"

#include "../common/exception.h"

#include "../optimizer/cost_model/default_cost_model.h"
#include "../optimizer/cost_model/postgres_cost_model.h"
#include "../optimizer/cost_model/trivial_cost_model.h"
#include "../optimizer/binding.h"
#include "../optimizer/input_column_deriver.h"
#include "../optimizer/operator_visitor.h"
#include "../optimizer/optimize_context.h"
#include "../optimizer/optimizer_task_pool.h"
#include "../optimizer/plan_generator.h"
#include "../optimizer/properties.h"
#include "../optimizer/property_enforcer.h"
#include "../optimizer/query_to_operator_transformer.h"
#include "../optimizer/rule.h"
#include "../optimizer/rule_impls.h"
#include "../parser/create_statement.h"

#include "../planner/analyze_plan.h"
#include "../planner/create_function_plan.h"
#include "../planner/create_plan.h"
#include "../planner/drop_plan.h"
#include "../planner/order_by_plan.h"
#include "../planner/populate_index_plan.h"
#include "../planner/projection_plan.h"
#include "../planner/seq_scan_plan.h"

#include "../storage/data_table.h"

using std::vector;
using std::unordered_map;
using std::shared_ptr;
using std::unique_ptr;
using std::move;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
Optimizer::Optimizer(const CostModels cost_model) : metadata_(nullptr) {

  switch (cost_model) {
    case CostModels::DEFAULT: {
       Debug("Using Default cost model. ");
      metadata_ = OptimizerMetadata(std::unique_ptr<AbstractCostModel>(new DefaultCostModel));
      break;
    }
    case CostModels::POSTGRES: {
       Debug("Using Postgres cost model. ");
      metadata_ = OptimizerMetadata(std::unique_ptr<AbstractCostModel>(new PostgresCostModel));
      break;
    }
    case CostModels::TRIVIAL: {
       Debug("Using Trivial cost model. ");
      metadata_ = OptimizerMetadata(std::unique_ptr<AbstractCostModel>(new TrivialCostModel));
      break;
    }
    default:
      throw OptimizerException("Invalid cost model");
  }
}

void Optimizer::OptimizeLoop(int root_group_id, std::shared_ptr<PropertySet> required_props) {
  //std::cerr << "call optimize loop" << std::endl;

  std::shared_ptr<OptimizeContext> root_context = std::make_shared<OptimizeContext>(&metadata_, required_props);
  auto task_stack = std::unique_ptr<OptimizerTaskStack>(new OptimizerTaskStack());
  metadata_.SetTaskPool(task_stack.get());

  // Perform rewrite first
  task_stack->Push(new TopDownRewrite(root_group_id, root_context, RewriteRuleSetName::PREDICATE_PUSH_DOWN));

  //FIXME: CAN DO WITHOUT?
  //task_stack->Push(new BottomUpRewrite(root_group_id, root_context, RewriteRuleSetName::UNNEST_SUBQUERY, false));

  //std::cerr << "ExecTaskStack1. " << std::endl;
  ExecuteTaskStack(*task_stack, root_group_id, root_context, 1);  //FIXME: TODO: FS: This seems to be expensive. Can we change this?

  // Perform optimization after the rewrite
  task_stack->Push(new OptimizeGroup(metadata_.memo.GetGroupByID(root_group_id), root_context));

    //FIXME: CAN DO WITHOUT?
  // Derive stats for the only one logical expression before optimizing
  //task_stack->Push(new DeriveStats(metadata_.memo.GetGroupByID(root_group_id)->GetLogicalExpression(), ExprSet{}, root_context));

  //std::cerr << "ExecTaskStack2. " << std::endl;
  ExecuteTaskStack(*task_stack, root_group_id, root_context, 2);  //FIXME: TODO: FS: This seems to be expensive. Can we change this?
}

shared_ptr<planner::AbstractPlan> Optimizer::BuildPelotonPlanTree(const std::unique_ptr<parser::SQLStatementList> &parse_tree_list,
    concurrency::TransactionContext *txn) {

  //std::cerr << "Start BuildPelotonPlanTree" << std::endl;
  if (parse_tree_list->GetStatements().empty()) {
    // TODO: create optimizer exception
    throw CatalogException("Parse tree list has no parse trees. Cannot build plan");
  }
  // TODO: support multi-statement queries
  auto parse_tree = parse_tree_list->GetStatement(0);
  //Debug("Parse Tree: %s ... (First 1000 char)", parse_tree->GetInfo().substr(0, 1000).c_str());

  unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  // Handle ddl statement
  bool is_ddl_stmt;
  auto ddl_plan = HandleDDLStatement(parse_tree, is_ddl_stmt, txn);
  if (is_ddl_stmt) {
    return move(ddl_plan);
  }

  metadata_.txn = txn;
  // Generate initial operator tree from query tree
  shared_ptr<GroupExpression> gexpr = InsertQueryTree(parse_tree, txn);  //FIXME: TODO: FS: This seems to be expensive. Can we change this?
  GroupID root_id = gexpr->GetGroupID();
  // Get the physical properties the final plan must output
  auto query_info = GetQueryInfo(parse_tree);


  //std::cerr << "query_info: " << query_info.physical_props->ToString() << std::endl;
 
 //TODO: Something before this also tries to call GetTableCatalog.???

  //Notice("Start OptimizeLoop");
  try {
    OptimizeLoop(root_id, query_info.physical_props);  //FIXME: TODO: FS: This seems to be expensive. Can we change this?
  } catch (OptimizerException &e) {
    LOG_WARN("Optimize Loop ended prematurely: %s", e.what());
  }

  //std::cerr << "query_info (post) " << query_info.physical_props->ToString() << std::endl;

  try {
    auto best_plan = ChooseBestPlan(root_id, query_info.physical_props, query_info.output_exprs);
    if (best_plan == nullptr) return nullptr;
    // Reset memo after finishing the optimization
    Reset();
    //  return shared_ptr<planner::AbstractPlan>(best_plan.release());
    return move(best_plan);
  } catch (Exception &e) {
    Reset();
    throw e;
  }
}

void Optimizer::Reset() {
  metadata_ = OptimizerMetadata(std::move(metadata_.cost_model));  //FIXME: TODO: FS: This seems to be expensive. Can we change this?
}

unique_ptr<planner::AbstractPlan> Optimizer::HandleDDLStatement(
    parser::SQLStatement *tree, bool &is_ddl_stmt,
    concurrency::TransactionContext *txn) {
  unique_ptr<planner::AbstractPlan> ddl_plan = nullptr;
  is_ddl_stmt = true;
  auto stmt_type = tree->GetType();
  switch (stmt_type) {
    case StatementType::DROP: {
      LOG_TRACE("Adding Drop plan...");
      unique_ptr<planner::AbstractPlan> drop_plan(new planner::DropPlan((parser::DropStatement *)tree));
      ddl_plan = move(drop_plan);
      break;
    }

    case StatementType::CREATE: {
      LOG_TRACE("Adding Create plan...");

      // This is adapted from the simple optimizer
      auto create_plan = new planner::CreatePlan((parser::CreateStatement *)tree);
      std::unique_ptr<planner::AbstractPlan> child_CreatePlan(create_plan);
      ddl_plan = move(child_CreatePlan);

      if (create_plan->GetCreateType() == peloton::CreateType::INDEX) {
        auto create_stmt = (parser::CreateStatement *)tree;
        auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
            txn,
            create_stmt->GetDatabaseName(),
            create_stmt->GetSchemaName(),
            create_stmt->GetTableName());
        std::vector<oid_t> column_ids;
        // use catalog object instead of schema to acquire metadata
        auto table_object = catalog::Catalog::GetInstance()->GetTableCatalogEntry(txn,
                                                                  create_stmt->GetDatabaseName(),
                                                                  create_stmt->GetSchemaName(),
                                                                  create_stmt->GetTableName());
        for (auto column_name : create_plan->GetIndexAttributes()) {
          auto column_object = table_object->GetColumnCatalogEntry(column_name);
          // Check if column is missing
          if (column_object == nullptr)
            throw CatalogException("Some columns are missing when create index " + std::string(create_stmt->index_name));
          oid_t col_pos = column_object->GetColumnId();
          column_ids.push_back(col_pos);
        }
        // Create a plan to retrieve data
        std::unique_ptr<planner::SeqScanPlan> child_SeqScanPlan(new planner::SeqScanPlan(target_table, nullptr, column_ids, false));

        child_SeqScanPlan->AddChild(std::move(ddl_plan));
        ddl_plan = std::move(child_SeqScanPlan);
        // Create a plan to add data to index
        std::unique_ptr<planner::AbstractPlan> child_PopulateIndexPlan(new planner::PopulateIndexPlan(target_table, column_ids));
        child_PopulateIndexPlan->AddChild(std::move(ddl_plan));
        create_plan->SetKeyAttrs(column_ids);
        ddl_plan = std::move(child_PopulateIndexPlan);
      }
      break;
    }
    case StatementType::TRANSACTION: {
      break;
    }
    case StatementType::CREATE_FUNC: {
      LOG_TRACE("Adding Create function plan...");
      unique_ptr<planner::AbstractPlan> create_func_plan(new planner::CreateFunctionPlan((parser::CreateFunctionStatement *)tree));
      ddl_plan = move(create_func_plan);
    } break;
    case StatementType::ANALYZE: {
      LOG_TRACE("Adding Analyze plan...");
      unique_ptr<planner::AbstractPlan> analyze_plan(new planner::AnalyzePlan(static_cast<parser::AnalyzeStatement *>(tree), txn));
      ddl_plan = move(analyze_plan);
      break;
    }
    default:
      is_ddl_stmt = false;
  }
  return ddl_plan;
}

shared_ptr<GroupExpression> Optimizer::InsertQueryTree(
    parser::SQLStatement *tree, concurrency::TransactionContext *txn) {
  QueryToOperatorTransformer converter(txn);
  shared_ptr<OperatorExpression> initial = converter.ConvertToOpExpression(tree);
  shared_ptr<GroupExpression> gexpr;
  metadata_.RecordTransformedExpression(initial, gexpr);
  return gexpr;
}

QueryInfo Optimizer::GetQueryInfo(parser::SQLStatement *tree) {
  auto GetQueryInfoHelper =
      [](std::vector<unique_ptr<expression::AbstractExpression>> &select_list,
         std::unique_ptr<parser::OrderDescription> &order_info,
         std::vector<expression::AbstractExpression *> &output_exprs,
         std::shared_ptr<PropertySet> &physical_props) {
        // Extract output column
        for (auto &expr : select_list) output_exprs.push_back(expr.get());

        // Extract sort property
        if (order_info != nullptr) {
          std::vector<expression::AbstractExpression *> sort_exprs;
          std::vector<bool> sort_ascending;
          for (auto &expr : order_info->exprs) {
            sort_exprs.push_back(expr.get());
          }
          for (auto &type : order_info->types) {
            sort_ascending.push_back(type == parser::kOrderAsc);
          }
          if (!sort_exprs.empty())
            physical_props->AddProperty(std::make_shared<PropertySort>(sort_exprs, sort_ascending));
        }
      };

  std::vector<expression::AbstractExpression *> output_exprs;
  std::shared_ptr<PropertySet> physical_props = std::make_shared<PropertySet>();
  switch (tree->GetType()) {
    case StatementType::SELECT: {
      auto *select = reinterpret_cast<parser::SelectStatement *>(tree);
      GetQueryInfoHelper(select->select_list, select->order, output_exprs, physical_props);
      break;
    }
    case StatementType::INSERT: {
      auto *insert = reinterpret_cast<parser::InsertStatement *>(tree);
      if (insert->select != nullptr)
        GetQueryInfoHelper(insert->select->select_list, insert->select->order, output_exprs, physical_props);
      break;
    }
    case StatementType::COPY: {
      auto *copy = reinterpret_cast<parser::CopyStatement *>(tree);
      if (copy->select_stmt != nullptr) {
        GetQueryInfoHelper(copy->select_stmt->select_list, copy->select_stmt->order, output_exprs, physical_props);
      } else {
        std::unique_ptr<parser::OrderDescription> order;
        GetQueryInfoHelper(copy->select_list, order, output_exprs, physical_props);
      }
      break;
    }
    default:
      break;
  }

  return QueryInfo(output_exprs, physical_props);
}

const std::string Optimizer::GetOperatorInfo(GroupID id, std::shared_ptr<PropertySet> required_props, int num_indent) {
    std::ostringstream os;

    Group *group = metadata_.memo.GetGroupByID(id);
    auto gexpr = group->GetBestExpression(required_props);
    
    os << std::endl << StringUtil::Indent(num_indent) << "operator name: " << gexpr->Op().GetName().c_str();

    vector<GroupID> child_groups = gexpr->GetChildGroupIDs();
    auto required_input_props = gexpr->GetInputProperties(required_props);
    PELOTON_ASSERT(required_input_props.size() == child_groups.size());

    for (size_t i = 0; i < child_groups.size(); ++i) {
        auto child_info = GetOperatorInfo(child_groups[i], required_input_props[i], num_indent + 2);
        os << StringUtil::Indent(num_indent + 2) << child_info;
    }
    return os.str();
}


unique_ptr<planner::AbstractPlan> Optimizer::ChooseBestPlan( GroupID id, std::shared_ptr<PropertySet> required_props, std::vector<expression::AbstractExpression *> required_cols) {

  // Notice("start ChooseBestPlan");
  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

  Group *group = metadata_.memo.GetGroupByID(id);
  LOG_TRACE("Choosing with property : %s", required_props->ToString().c_str());
  auto gexpr = group->GetBestExpression(required_props);

  Debug("GEXPR: %s", gexpr->Op().GetName().c_str());

  LOG_TRACE("Choosing best plan for group %d with op %s", gexpr->GetGroupID(), gexpr->Op().GetName().c_str());

  vector<GroupID> child_groups = gexpr->GetChildGroupIDs();
  auto required_input_props = gexpr->GetInputProperties(required_props);
  PELOTON_ASSERT(required_input_props.size() == child_groups.size());
  // Firstly derive input/output columns
  InputColumnDeriver deriver;
  auto output_input_cols_pair = deriver.DeriveInputColumns(gexpr, required_props, required_cols, &metadata_.memo);
  auto &output_cols = output_input_cols_pair.first;
  auto &input_cols = output_input_cols_pair.second;
  PELOTON_ASSERT(input_cols.size() == required_input_props.size());

  // Derive chidren plans first because they are useful in the derivation of root plan. Also keep propagate expression to column offset mapping
  vector<unique_ptr<planner::AbstractPlan>> children_plans;
  vector<ExprMap> children_expr_map;
  for (size_t i = 0; i < child_groups.size(); ++i) {
    ExprMap child_expr_map;
    for (unsigned offset = 0; offset < input_cols[i].size(); ++offset) {
      PELOTON_ASSERT(input_cols[i][offset] != nullptr);
      child_expr_map[input_cols[i][offset]] = offset;
    }
    auto child_plan = ChooseBestPlan(child_groups[i], required_input_props[i], input_cols[i]);
    children_expr_map.push_back(move(child_expr_map));
    PELOTON_ASSERT(child_plan != nullptr);
    children_plans.push_back(move(child_plan));
  }

  // Derive root plan
  shared_ptr<OperatorExpression> op =make_shared<OperatorExpression>(gexpr->Op());

  Debug("OP: %s", op->GetInfo().c_str());

  PlanGenerator generator;
  auto plan = generator.ConvertOpExpression(op, required_props, required_cols,
                                            output_cols, children_plans,
                                            children_expr_map, group->GetNumRows());
  LOG_TRACE("Finish Choosing best plan for group %d", id);


  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_end = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  // Warning("ChooseBestPlan took %d us", microseconds_end-microseconds_start);

  return plan;
}

void Optimizer::ExecuteTaskStack(OptimizerTaskStack &task_stack, int root_group_id, std::shared_ptr<OptimizeContext> root_context, int i) {

  //std::cerr << "call execute task stack" << std::endl;
  auto root_group = metadata_.memo.GetGroupByID(root_group_id);
  auto &timer = metadata_.timer;
  const auto timeout_limit = metadata_.timeout_limit;
  const auto &required_props = root_context->required_prop;


   //TESTING HOW LONG THIS TAKES: FIXME: REMOVE 
  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

  //FIXME: FS: Timer seems unecessary?
  // if (timer.GetInvocations() == 0) {
  //   timer.Start();
  // }
  // Iterate through the task stack
  //std::cerr << "task stack size: " << task_stack.Size() << std::endl;
  while (!task_stack.Empty()) {
    // Check to see if we have at least one plan, and if we have exceeded our timeout limit
    // if (timer.GetDuration() >= timeout_limit && root_group->HasExpressions(required_props)) {
    //   throw OptimizerException("Optimizer task execution duration " + std::to_string(timer.GetDuration()) + " exceeds timeout limit " + std::to_string(timeout_limit));
    // }
    // timer.Reset();
    auto task = task_stack.Pop();
    //std::cerr << "task stack size remaining: " << task_stack.Size() << std::endl;
    
  
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start2 = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

    task->execute();

  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_end2 = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
 
  // //Should not take more than 1 ms (already generous) to parse and prepare.
  // auto duration2 = microseconds_end2 - microseconds_start2;
  //  Warning("TaskExecute duration: %d us", duration2);
  // if(duration2 > 50){
  //   Warning("TaskExecute exceeded 50us: %d us", duration2);
  // }

    //std::cerr << "task stack size remaining (post execute): " << task_stack.Size() << std::endl;
    //timer.Stop();
  }


  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_end = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
 
  // //Should not take more than 1 ms (already generous) to parse and prepare.
  // auto duration = microseconds_end - microseconds_start;
  // if(duration > 300){
  //   Warning("ExecuteTaskStack%d exceeded 300us: %d us", i, duration);
  // }
}

}  // namespace optimizer
}  // namespace peloton
