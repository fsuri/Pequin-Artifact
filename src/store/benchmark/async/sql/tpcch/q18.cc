#include "store/benchmark/async/sql/tpcch/q18.h"

namespace tpcch_sql {

Q18::Q18(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q18::~Q18() {}

transaction_status_t Q18::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT customer.last, "
                     "customer.id, "
                     "\"order\".id, "
                     "\"order\".entry_d, "
                     "\"order\".ol_cnt, "
                     "sum(order_line.amount) AS amount_sum "
                     "FROM customer, "
                     "\"order\", "
                     "order_line "
                     "WHERE customer.id = \"order\".c_id "
                     "AND customer.w_id = \"order\".w_id "
                     "AND customer.d_id = \"order\".d_id "
                     "AND order_line.w_id = \"order\".w_id "
                     "AND order_line.d_id = \"order\".d_id "
                     "AND order_line.o_id = \"order\".id "
                     "GROUP BY \"order\".id, "
                     "\"order\".w_id, "
                     "\"order\".d_id, "
                     "customer.id, "
                     "customer.last, "
                     "\"order\".entry_d, "
                     "\"order\".ol_cnt HAVING sum(order_line.amount) > 200 "
                     "ORDER BY amount_sum DESC, \"order\".entry_d";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}