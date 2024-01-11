#include "store/benchmark/async/sql/tpcch/q3.h"

namespace tpcch_sql {

Q3::Q3(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q3::~Q3() {}

transaction_status_t Q3::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT order_line.o_id, "
                    "order_line.w_id, "
                    "order_line.d_id, "
                    "sum(order_line.amount) AS revenue, "
                     "\"order\".entry_d "
                     "FROM customer, "
                     "new_order, "
                     "\"order\", "
                     "order_line "
                     "WHERE customer.state LIKE 'A%' "
                     "AND customer.id = \"order\".c_id "
                     "AND customer.w_id = \"order\".w_id "
                     "AND customer.d_id = \"order\".d_id "
                     "AND new_order.w_id = \"order\".w_id "
                     "AND new_order.d_id = \"order\".d_id "
                     "AND new_order.o_id = \"order\".id "
                     "AND order_line.w_id = \"order\".w_id "
                     "AND order_line.d_id = \"order\".d_id "
                     "AND order_line.o_id = \"order\".id "
                     "AND \"order\".entry_d > 1167714000 "
                     "GROUP BY order_line.o_id, "
                     "order_line.w_id, "
                     "order_line.d_id, "
                     "\"order\".entry_d "
                     "ORDER BY revenue DESC , \"order\".entry_d";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


