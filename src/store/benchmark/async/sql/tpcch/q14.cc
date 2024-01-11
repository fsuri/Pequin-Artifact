#include "store/benchmark/async/sql/tpcch/q14.h"

namespace tpcch_sql {

Q14::Q14(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q14::~Q14() {}

transaction_status_t Q14::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT (100.00 * sum(CASE WHEN item.data LIKE 'PR%' THEN order_line.amount ELSE 0 END) / (1 + sum(order_line.amount))) AS promo_revenue "
                    "FROM order_line, "
                    "item "
                    "WHERE order_line.i_id = item.id "
                    "AND order_line.delivery_d >= 1167714000 "
                    "AND order_line.delivery_d < 1577941200";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}