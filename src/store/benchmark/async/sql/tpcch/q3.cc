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
                     "oorder.entry_d "
                     "FROM customer, "
                     "new_order, "
                     "oorder, "
                     "order_line "
                     "WHERE customer.state LIKE 'A%' "
                     "AND customer.id = oorder.c_id "
                     "AND customer.w_id = oorder.w_id "
                     "AND customer.d_id = oorder.d_id "
                     "AND new_order.w_id = oorder.w_id "
                     "AND new_order.d_id = oorder.d_id "
                     "AND new_order.o_id = oorder.id "
                     "AND order_line.w_id = oorder.w_id "
                     "AND order_line.d_id = oorder.d_id "
                     "AND order_line.o_id = oorder.id "
                     "AND oorder.entry_d > 1167714000 "  //2007-01-02 00:00:00.000000 in seconds since the epoch (1970) (Using EST here to compare to Local Time Zone which is EST)
                     "GROUP BY order_line.o_id, "
                     "order_line.w_id, "
                     "order_line.d_id, "
                     "oorder.entry_d "
                     "ORDER BY revenue DESC , oorder.entry_d";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


