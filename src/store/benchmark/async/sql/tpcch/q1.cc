#include "store/benchmark/async/sql/tpcch/q1.h"

namespace tpcch_sql {

Q1::Q1(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q1::~Q1() {}

transaction_status_t Q1::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT number, " 
                    "sum(quantity) AS sum_qty, "
                    "sum(amount) AS sum_amount, "
                    "avg(quantity) AS avg_qty, "
                    "avg(amount) AS avg_amount, "
                    "count(*) AS count_order "
                    "FROM order_line "
                    "WHERE delivery_d > 1167714000"
                    "GROUP BY number "
                    "ORDER BY number";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    std::cerr << "Done" << std::endl;
    return client.Commit(timeout);
}
}


