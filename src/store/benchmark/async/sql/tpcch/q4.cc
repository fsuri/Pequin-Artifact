#include "store/benchmark/async/sql/tpcch/q4.h"

namespace tpcch_sql {

Q4::Q4(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q4::~Q4() {}

transaction_status_t Q4::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT o_ol_cnt, "
                     "count(*) AS order_count "
                     "FROM oorder "
                     "WHERE exists "
                     "(SELECT * "
                     "FROM order_line "
                     "WHERE o_id = ol_o_id "
                     "AND o_w_id = ol_w_id "
                     "AND o_d_id = ol_d_id "
                     "AND ol_delivery_d >= o_entry_d) "
                     "GROUP BY o_ol_cnt "
                     "ORDER BY o_ol_cnt";
    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


