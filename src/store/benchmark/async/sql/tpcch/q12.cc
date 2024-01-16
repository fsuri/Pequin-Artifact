#include "store/benchmark/async/sql/tpcch/q12.h"

namespace tpcch_sql {

Q12::Q12(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q12::~Q12() {}

transaction_status_t Q12::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT oorder.ol_cnt, "
                     "sum(CASE WHEN oorder.carrier_id = 1 "
                     "OR oorder.carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count, "
                     "sum(CASE WHEN oorder.carrier_id <> 1 "
                     "AND oorder.carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count "
                     "FROM oorder, "
                     "order_line "
                     "WHERE order_line.w_id = oorder.w_id "
                     "AND order_line.d_id = oorder.d_id "
                     "AND order_line.o_id = oorder.id "
                     "AND oorder.entry_d <= order_line.delivery_d "
                     "AND order_line.delivery_d < 1577854800" //2020-01-01 00:00:00.000000' " in seconds since EPOCH (EST)
                     "GROUP BY oorder.ol_cnt "
                     "ORDER BY oorder.ol_cnt";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}