#include "store/benchmark/async/sql/tpcch/q5.h"

namespace tpcch_sql {

Q5::Q5(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q5::~Q5() {}

transaction_status_t Q5::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT n_name, "
                     "sum(ol_amount) AS revenue "
                     "FROM customer, "
                     "oorder, "
                     "order_line, "
                     "stock, "
                     "supplier, "
                     "nation, "
                     "region "
                     "WHERE c_id = o_c_id "
                     "AND c_w_id = o_w_id "
                     "AND c_d_id = o_d_id "
                     "AND ol_o_id = o_id "
                     "AND ol_w_id = o_w_id "
                     "AND ol_d_id = o_d_id "
                     "AND ol_w_id = s_w_id "
                     "AND ol_i_id = s_i_id "
                     "AND MOD((s_w_id * s_i_id), 10000) = su_suppkey "
                     "AND ascii(substring(c_state from 1 for 1)) = su_nationkey "
                     "AND su_nationkey = n_nationkey "
                     "AND n_regionkey = r_regionkey "
                     "AND r_name = 'Europe' "
                     "AND o_entry_d >= 1167714000"  //2007-01-02 00:00:00.000000 in seconds since the epoch (1970) (Using EST here to compare to Local Time Zone which is EST)
                     "GROUP BY n_name "
                     "ORDER BY revenue DESC";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


