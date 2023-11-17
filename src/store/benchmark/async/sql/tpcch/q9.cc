#include "store/benchmark/async/sql/tpcch/q9.h"

namespace tpcch_sql {

Q9::Q9(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q9::~Q9() {}

transaction_status_t Q9::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT n_name, "
                     "extract(YEAR "
                     "FROM o_entry_d) AS l_year, "
                     "sum(ol_amount) AS sum_profit "
                     "FROM item, "
                     "stock, "
                     "supplier, "
                     "order_line, "
                     "oorder, "
                     "nation "
                     "WHERE ol_i_id = s_i_id "
                     "AND ol_supply_w_id = s_w_id "
                     "AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey "
                     "AND ol_w_id = o_w_id "
                     "AND ol_d_id = o_d_id "
                     "AND ol_o_id = o_id "
                     "AND ol_i_id = i_id "
                     "AND su_nationkey = n_nationkey "
                     "AND i_data LIKE '%bb' "
                     "GROUP BY n_name, "
                     "l_year "
                     "ORDER BY n_name, "
                     "l_year DESC";

    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}