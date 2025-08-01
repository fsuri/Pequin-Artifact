#include "store/benchmark/async/sql/tpcch/q21.h"

namespace tpcch_sql {

Q21::Q21(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q21::~Q21() {}

transaction_status_t Q21::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT su_name, "
                     "count(*) AS numwait "
                     "FROM supplier, "
                     "order_line l1, "
                     "oorder, "
                     "stock, "
                     "nation "
                     "WHERE l1.o_id = o_id "
                     "AND l1.w_id = o_w_id "
                     "AND l1.d_id = o_d_id "
                     "AND l1.w_id = s_w_id "
                     "AND l1.i_id = s_i_id "
                     "AND mod((s_w_id * s_i_id),10000) = su_suppkey "
                     "AND l1.delivery_d > o_entry_d "
                     "AND NOT EXISTS "
                     "(SELECT * "
                     "FROM order_line l2 "
                     "WHERE l2.o_id = l1.o_id "
                     "AND l2.w_id = l1.w_id "
                     "AND l2.d_id = l1.d_id "
                     "AND l2.delivery_d > l1.delivery_d) "
                     "AND su_nationkey = n_nationkey "
                     "AND n_name = 'Germany' "
                     "GROUP BY su_name "
                     "ORDER BY numwait DESC, su_name";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}