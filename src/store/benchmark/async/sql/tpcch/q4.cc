#include "store/benchmark/async/sql/tpcch/q4.h"

namespace tpcch_sql {

Q4::Q4(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q4::~Q4() {}

transaction_status_t Q4::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT order.ol_cnt, "
                     "count(*) AS order_count "
                     "FROM \"order\" "
                     "WHERE exists "
                     "(SELECT * "
                     "FROM order_line "
                     "WHERE \"order\".id = order_line.o_id "
                     "AND \"order\".w_id = order_line.w_id "
                     "AND \"order\".d_id = order_line.d_id "
                     "AND order_line.delivery_d >= \"order\".entry_d) "
                     "GROUP BY \"order\".ol_cnt "
                     "ORDER BY \"order\".ol_cnt";
    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


