//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_set_op_executor.cpp
//
// Identification: src/executor/hash_set_op_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../common/logger.h"
#include "../type/value.h"
#include "../executor/logical_tile.h"
#include "../executor/hash_set_op_executor.h"

#include "../planner/set_op_plan.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
HashSetOpExecutor::HashSetOpExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool HashSetOpExecutor::DInit() {
  PELOTON_ASSERT(children_.size() == 2);
  PELOTON_ASSERT(!hash_done_);
  PELOTON_ASSERT(set_op_ == SetOpType::INVALID);

  return true;
}

bool HashSetOpExecutor::DExecute() {
  LOG_TRACE("Set Op executor ");

  if (!hash_done_) ExecuteHelper();

  PELOTON_ASSERT(hash_done_);

  // Avoid returning empty tiles
  while (next_tile_to_return_ < left_tiles_.size()) {
    if (left_tiles_[next_tile_to_return_]->GetTupleCount() > 0) {
      SetOutput(left_tiles_[next_tile_to_return_].release());
      next_tile_to_return_++;
      return true;
    } else
      next_tile_to_return_++;
  }
  return false;
}

bool HashSetOpExecutor::ExecuteHelper() {
  PELOTON_ASSERT(children_.size() == 2);
  PELOTON_ASSERT(!hash_done_);

  // Grab data from plan node
  const planner::SetOpPlan &node = GetPlanNode<planner::SetOpPlan>();
  set_op_ = node.GetSetOp();
  PELOTON_ASSERT(set_op_ != SetOpType::INVALID);

  // Extract all input from left child
  while (children_[0]->Execute()) {
    left_tiles_.emplace_back(children_[0]->GetOutput());
  }

  if (left_tiles_.size() == 0) return false;

  // Scan the left child's input and update the counters
  for (auto &tile : left_tiles_) {
    for (oid_t tuple_id : *tile) {
      htable_[HashSetOpMapType::key_type(tile.get(), tuple_id)].left++;
    }
  }

  // Scan the right child's input and update counter when appropriate
  while (children_[1]->Execute()) {
    // Each right tile can be destroyed after processing
    std::unique_ptr<LogicalTile> tile(children_[1]->GetOutput());

    for (oid_t tuple_id : *tile) {
      auto it = htable_.find(HashSetOpMapType::key_type(tile.get(), tuple_id));
      // Do nothing if this key never appears in the left child
      // because it shouldn't show up in the result anyway
      if (it != htable_.end()) {
        it->second.right++;
      }
    }
  }

  // Calculate the output number for each key
  switch (set_op_) {
    case SetOpType::INTERSECT:
      CalculateCopies<SetOpType::INTERSECT>(htable_);
      break;
    case SetOpType::INTERSECT_ALL:
      CalculateCopies<SetOpType::INTERSECT_ALL>(htable_);
      break;
    case SetOpType::EXCEPT:
      CalculateCopies<SetOpType::EXCEPT>(htable_);
      break;
    case SetOpType::EXCEPT_ALL:
      CalculateCopies<SetOpType::EXCEPT_ALL>(htable_);
      break;
    case SetOpType::INVALID:
      return false;
  }

  /*
   * A bit cumbersome here:
   * Since the first tuple of each group is used as the representative key
   * in the hash table,
   * we cannot invalidate it otherwise subsequent key comparison will fail
   * (due to safety check in LogicalTile::GetValue()).
   * Instead, we skip the first tuple and process it in a second round.
   */

  // 1st round
  for (auto &tile : left_tiles_) {
    for (oid_t tuple_id : *tile) {
      auto it = htable_.find(HashSetOpMapType::key_type(tile.get(), tuple_id));

      PELOTON_ASSERT(it != htable_.end());

      if (it->first.GetContainer() == tile.get() &&
          it->first.GetTupleId() == tuple_id)
        continue;
      else if (it->second.left > 0)
        it->second.left--;
      else
        tile->RemoveVisibility(tuple_id);
    }
  }

  // 2nd round
  for (auto &item : htable_) {
    // We should have at most one quota left
    PELOTON_ASSERT(item.second.left == 1 || item.second.left == 0);
    if (item.second.left == 0) {
      item.first.GetContainer()->RemoveVisibility(item.first.GetTupleId());
    }
  }

  hash_done_ = true;
  next_tile_to_return_ = 0;
  return true;
}

/**
 * Based on the set-op type,
 * calculate the number of output copies of each tuples
 * and store it in the left counter.
 */
template <SetOpType SETOP>
bool HashSetOpExecutor::CalculateCopies(HashSetOpMapType &htable) {
  for (auto &item : htable) {
    switch (SETOP) {
      case SetOpType::INTERSECT:
        item.second.left = (item.second.right > 0) ? 1 : 0;
        break;
      case SetOpType::INTERSECT_ALL:
        item.second.left = std::min(item.second.left, item.second.right);
        break;
      case SetOpType::EXCEPT:
        item.second.left = (item.second.right > 0) ? 0 : 1;
        break;
      case SetOpType::EXCEPT_ALL:
        item.second.left = (item.second.left > item.second.right)
                               ? (item.second.left - item.second.right)
                               : 0;
        break;
      default:
        return false;
    }
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
