#include "store/benchmark/async/sql/tpcch/q10.h"

namespace tpcch_sql {

Q10::Q10(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q10::~Q10() {}

transaction_status_t Q10::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT c_id, "
                     "c_last, "
                     "sum(ol_amount) AS revenue, "
                     "c_city, "
                     "c_phone, "
                     "n_name "
                     "FROM customer, "
                     "oorder, "
                     "order_line, "
                     "nation "
                     "WHERE c_id = o_c_id "
                     "AND c_w_id = o_w_id "
                     "AND c_d_id = o_d_id "
                     "AND ol_w_id = o_w_id "
                     "AND ol_d_id = o_d_id "
                     "AND ol_o_id = o_id "
                     "AND o_entry_d >= 1167714000"  //2007-01-02 00:00:00.000000 in seconds since the epoch (1970) (Using EST here to compare to Local Time Zone which is EST)
                     "AND o_entry_d <= ol_delivery_d "
                     "AND n_nationkey = ascii(substring(c_state from  1  for  1)) "
                     "GROUP BY c_id, "
                     "c_last, "
                     "c_city, "
                     "c_phone, "
                     "n_name "
                     "ORDER BY revenue DESC";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}