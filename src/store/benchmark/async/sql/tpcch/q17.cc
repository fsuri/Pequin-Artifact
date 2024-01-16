#include "store/benchmark/async/sql/tpcch/q17.h"

namespace tpcch_sql {

Q17::Q17(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q17::~Q17() {}

transaction_status_t Q17::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT SUM(order_line.amount) / 2.0 AS avg_yearly "
                     "FROM order_line, "
                     "(SELECT item.id, AVG (order_line.quantity) AS a "
                     "FROM item, "
                     "order_line "
                     "WHERE item.data LIKE '%b' "
                     "AND order_line.i_id = item.id "
                     "GROUP BY item.id) t "
                     "WHERE order_line.i_id = t.id "
                     "AND order_line.quantity < t.a";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}