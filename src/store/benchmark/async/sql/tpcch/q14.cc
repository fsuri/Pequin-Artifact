#include "store/benchmark/async/sql/tpcch/q14.h"

namespace tpcch_sql {

Q14::Q14(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q14::~Q14() {}

transaction_status_t Q14::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue "
                    "FROM order_line, "
                    "item "
                    "WHERE ol_i_id = i_id "
                    "AND ol_delivery_d >= 1167714000 " //'2007-01-02 00:00:00.000000'
                    "AND ol_delivery_d < 1577941200"; ////2020-01-02 00:00:00.000000' " in seconds since EPOCH (EST)

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}