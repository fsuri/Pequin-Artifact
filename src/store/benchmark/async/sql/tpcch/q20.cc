#include "store/benchmark/async/sql/tpcch/q20.h"

namespace tpcch_sql {

Q20::Q20(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q20::~Q20() {}

transaction_status_t Q20::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT su_name, "
                     "su_address "
                     "FROM supplier, "
                     "nation "
                     "WHERE su_suppkey IN "
                     "(SELECT mod(s_i_id * s_w_id, 10000) "
                     "FROM stock "
                     "INNER JOIN item ON i_id = s_i_id "
                     "INNER JOIN order_line ON ol_i_id = s_i_id "
                     "WHERE ol_delivery_d > 1274630400 "  //2010-05-23 12:00:00
                     "AND i_data LIKE 'co%' "
                     "GROUP BY s_i_id, "
                     "s_w_id, "
                     "s_quantity HAVING 2*s_quantity > sum(ol_quantity)) "
                     "AND su_nationkey = n_nationkey "
                     "AND n_name = 'Germany' "
                     "ORDER BY su_name";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}