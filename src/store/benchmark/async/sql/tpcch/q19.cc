#include "store/benchmark/async/sql/tpcch/q19.h"

namespace tpcch_sql {

Q19::Q19(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q19::~Q19() {}

transaction_status_t Q19::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT sum(ol_amount) AS revenue "
                     "FROM order_line, "
                     "item "
                     "WHERE (ol_i_id = i_id "
                     "AND i_data LIKE '%a' "
                     "AND ol_quantity >= 1 "
                     "AND ol_quantity <= 10 "
                     "AND i_price BETWEEN 1 AND 400000 "
                     "AND ol_w_id IN (1, "
                     "2, "
                     "3)) "
                     "OR (ol_i_id = i_id "
                     "AND i_data LIKE '%b' "
                     "AND ol_quantity >= 1 "
                     "AND ol_quantity <= 10 "
                     "AND i_price BETWEEN 1 AND 400000 "
                     "AND ol_w_id IN (1, "
                     "2, "
                     "4)) "
                     "OR (ol_i_id = i_id "
                     "AND i_data LIKE '%c' "
                     "AND ol_quantity >= 1 "
                     "AND ol_quantity <= 10 "
                     "AND i_price BETWEEN 1 AND 400000 "
                     "AND ol_w_id IN (1, "
                     "5, "
                     "3))";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}