#include "store/benchmark/async/sql/tpcch/q11.h"

namespace tpcch_sql {

Q11::Q11(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q11::~Q11() {}

transaction_status_t Q11::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT s_i_id, "
                     "sum(s_order_cnt) AS ordercount "
                     "FROM stock, "
                     "supplier, "
                     "nation "
                     "WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey "
                     "AND su_nationkey = n_nationkey "
                     "AND n_name = 'Germany' "
                     "GROUP BY s_i_id HAVING sum(s_order_cnt) > "
                     "(SELECT sum(s_order_cnt) * .005 "
                     "FROM stock, "
                     "supplier, "
                     "nation "
                     "WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey "
                     "AND su_nationkey = n_nationkey "
                     "AND n_name = 'Germany') "
                     "ORDER BY ordercount DESC";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}