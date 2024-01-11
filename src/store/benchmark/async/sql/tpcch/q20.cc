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
                     "(SELECT mod(stock.i_id * stock.w_id, 10000) "
                     "FROM stock "
                     "INNER JOIN item ON item.id = stock.i_id "
                     "INNER JOIN order_line ON order_line.i_id = stock.i_id "
                     "WHERE order_line.delivery_d > 1274630400 "
                     "AND item.data LIKE 'co%' "
                     "GROUP BY stock.i_id, "
                     "stock.w_id, "
                     "stock.quantity HAVING 2*stock.quantity > sum(order_line.quantity)) "
                     "AND su_nationkey = n_nationkey "
                     "AND n_name = 'Germany' "
                     "ORDER BY su_name";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}