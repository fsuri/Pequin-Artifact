#include "store/benchmark/async/sql/tpcch/q12.h"

namespace tpcch_sql {

Q12::Q12(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q12::~Q12() {}

transaction_status_t Q12::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT \"order\".ol_cnt, "
                     "sum(CASE WHEN \"order\".carrier_id = 1 "
                     "OR \"order\".carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count, "
                     "sum(CASE WHEN \"order\".carrier_id <> 1 "
                     "AND \"order\".carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count "
                     "FROM \"order\", "
                     "order_line "
                     "WHERE order_line.w_id = \"order\".w_id "
                     "AND order_line.d_id = \"order\".d_id "
                     "AND order_line.o_id = \"order\".id "
                     "AND \"order\".entry_d <= order_line.delivery_d "
                     "AND order_line.delivery_d < 1577854800"
                     "GROUP BY \"order\".ol_cnt "
                     "ORDER BY \"order\".ol_cnt";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}