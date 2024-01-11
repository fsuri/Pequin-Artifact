#include "store/benchmark/async/sql/tpcch/q6.h"

namespace tpcch_sql {

Q6::Q6(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q6::~Q6() {}

transaction_status_t Q6::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT sum(order_line.amount) AS revenue "
                     "FROM order_line "
                     "WHERE order_line.delivery_d >= 915166800 "
                     "AND order_line.delivery_d < 1577854800 "
                     "AND order_line.quantity BETWEEN 1 AND 100000";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}