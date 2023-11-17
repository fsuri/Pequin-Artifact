#include "store/benchmark/async/sql/tpcch/q17.h"

namespace tpcch_sql {

Q17::Q17(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q17::~Q17() {}

transaction_status_t Q17::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT SUM(ol_amount) / 2.0 AS avg_yearly "
                     "FROM order_line, "
                     "(SELECT i_id, AVG (ol_quantity) AS a "
                     "FROM item, "
                     "order_line "
                     "WHERE i_data LIKE '%b' "
                     "AND ol_i_id = i_id "
                     "GROUP BY i_id) t "
                     "WHERE ol_i_id = t.i_id "
                     "AND ol_quantity < t.a";

    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}