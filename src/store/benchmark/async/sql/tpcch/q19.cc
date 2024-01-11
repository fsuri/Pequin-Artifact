#include "store/benchmark/async/sql/tpcch/q19.h"

namespace tpcch_sql {

Q19::Q19(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q19::~Q19() {}

transaction_status_t Q19::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT sum(order_line.amount) AS revenue "
                     "FROM order_line, "
                     "item "
                     "WHERE (order_line.i_id = item.id "
                     "AND item.data LIKE '%a' "
                     "AND order_line.quantity >= 1 "
                     "AND order_line.quantity <= 10 "
                     "AND item.price BETWEEN 1 AND 400000 "
                     "AND order_line.w_id IN (1, "
                     "2, "
                     "3)) "
                     "OR (order_line.i_id = i_id "
                     "AND item.data LIKE '%b' "
                     "AND order_line.quantity >= 1 "
                     "AND order_line.quantity <= 10 "
                     "AND item.price BETWEEN 1 AND 400000 "
                     "AND order_line.w_id IN (1, "
                     "2, "
                     "4)) "
                     "OR (order_line.i_id = item.id "
                     "AND item.data LIKE '%c' "
                     "AND order_line.quantity >= 1 "
                     "AND order_line.quantity <= 10 "
                     "AND item.price BETWEEN 1 AND 400000 "
                     "AND order_line.w_id IN (1, "
                     "5, "
                     "3))";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}