#include "store/benchmark/async/sql/tpcch/q13.h"

namespace tpcch_sql {

Q13::Q13(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q13::~Q13() {}

transaction_status_t Q13::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT c_count, "
                     "count(*) AS custdist "
                     "FROM "
                     "(SELECT c_id, "
                     "count(o_id) AS c_count "
                     "FROM customer "
                     "LEFT OUTER JOIN oorder ON (c_w_id = o_w_id "
                     "AND c_d_id = o_d_id "
                     "AND c_id = o_c_id "
                     "AND o_carrier_id > 8) "
                     "GROUP BY c_id) AS c_orders "
                     "GROUP BY c_count "
                     "ORDER BY custdist DESC, c_count DESC";

    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}