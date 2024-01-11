#include "store/benchmark/async/sql/tpcch/q13.h"

namespace tpcch_sql {

Q13::Q13(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q13::~Q13() {}

transaction_status_t Q13::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT c_count, "
                     "count(*) AS custdist "
                     "FROM "
                     "(SELECT customer.id, "
                     "count(\"order\".id) AS c_count "
                     "FROM customer "
                     "LEFT OUTER JOIN \"order\" ON (customer.w_id = \"order\".w_id "
                     "AND customer.d_id = \"order\".d_id "
                     "AND customer.id = \"order\".c_id "
                     "AND \"order\".carrier_id > 8) "
                     "GROUP BY customer.id) AS c_orders "
                     "GROUP BY c_count "
                     "ORDER BY custdist DESC, c_count DESC";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}