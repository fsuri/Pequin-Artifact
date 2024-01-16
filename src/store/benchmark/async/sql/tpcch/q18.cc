#include "store/benchmark/async/sql/tpcch/q18.h"

namespace tpcch_sql {

Q18::Q18(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q18::~Q18() {}

transaction_status_t Q18::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT customer.last, "
                     "customer.id, "
                     "oorder.id, "
                     "oorder.entry_d, "
                     "oorder.ol_cnt, "
                     "sum(order_line.amount) AS amount_sum "
                     "FROM customer, "
                     "oorder, "
                     "order_line "
                     "WHERE customer.id = oorder.c_id "
                     "AND customer.w_id = oorder.w_id "
                     "AND customer.d_id = oorder.d_id "
                     "AND order_line.w_id = oorder.w_id "
                     "AND order_line.d_id = oorder.d_id "
                     "AND order_line.o_id = oorder.id "
                     "GROUP BY oorder.id, "
                     "oorder.w_id, "
                     "oorder.d_id, "
                     "customer.id, "
                     "customer.last, "
                     "oorder.entry_d, "
                     "oorder.ol_cnt HAVING sum(order_line.amount) > 200 "
                     "ORDER BY amount_sum DESC, oorder.entry_d";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}