#include "store/benchmark/async/sql/tpcch/q6.h"

namespace tpcch_sql {

Q6::Q6(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q6::~Q6() {}

transaction_status_t Q6::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT sum(ol_amount) AS revenue "
                     "FROM order_line "
                     "WHERE ol_delivery_d >= 915166800 "  //1999-01-01 00:00:00.000000' " in seconds since EPOCH (EST)
                     "AND ol_delivery_d < 1577854800 "    //2020-01-01 00:00:00.000000' " in seconds since EPOCH (EST)
                     "AND ol_quantity BETWEEN 1 AND 100000";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}