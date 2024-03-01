#include "store/benchmark/async/sql/tpcch/q18.h"

namespace tpcch_sql {

Q18::Q18(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q18::~Q18() {}

transaction_status_t Q18::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT c_last, "
                     "c_id, "
                     "o_id, "
                     "o_entry_d, "
                     "o_ol_cnt, "
                     "sum(ol_amount) AS amount_sum "
                     "FROM customer, "
                     "oorder, "
                     "order_line "
                     "WHERE c_id = o_c_id "
                     "AND c_w_id = o_w_id "
                     "AND c_d_id = o_d_id "
                     "AND ol_w_id = o_w_id "
                     "AND ol_d_id = o_d_id "
                     "AND ol_o_id = o_id "
                     "GROUP BY o_id, "
                     "o_w_id, "
                     "o_d_id, "
                     "c_id, "
                     "c_last, "
                     "o_entry_d, "
                     "o_ol_cnt HAVING sum(ol_amount) > 200 "
                     "ORDER BY amount_sum DESC, o_entry_d";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}