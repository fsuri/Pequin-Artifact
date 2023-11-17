#include "store/benchmark/async/sql/tpcch/q3.h"

namespace tpcch_sql {

Q3::Q3(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q3::~Q3() {}

transaction_status_t Q3::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT ol_o_id, "
                    "ol_w_id, "
                    "ol_d_id, "
                    "sum(ol_amount) AS revenue, "
                     "o_entry_d "
                     "FROM customer, "
                     "new_order, "
                     "oorder, "
                     "order_line "
                     "WHERE c_state LIKE 'A%' "
                     "AND c_id = o_c_id "
                     "AND c_w_id = o_w_id "
                     "AND c_d_id = o_d_id "
                     "AND no_w_id = o_w_id "
                     "AND no_d_id = o_d_id "
                     "AND no_o_id = o_id "
                     "AND ol_w_id = o_w_id "
                     "AND ol_d_id = o_d_id "
                     "AND ol_o_id = o_id "
                     "AND o_entry_d > '2007-01-02 00:00:00.000000' "
                     "GROUP BY ol_o_id, "
                     "ol_w_id, "
                     "ol_d_id, "
                     "o_entry_d "
                     "ORDER BY revenue DESC , o_entry_d";

    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


