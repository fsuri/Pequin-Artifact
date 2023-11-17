#include "store/benchmark/async/sql/tpcch/q6.h"

namespace tpcch_sql {

Q6::Q6(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q6::~Q6() {}

transaction_status_t Q6::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT sum(ol_amount) AS revenue "
                     "FROM order_line "
                     "WHERE ol_delivery_d >= '1999-01-01 00:00:00.000000' "
                     "AND ol_delivery_d < '2020-01-01 00:00:00.000000' "
                     "AND ol_quantity BETWEEN 1 AND 100000";

    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}