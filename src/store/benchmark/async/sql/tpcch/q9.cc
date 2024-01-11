#include "store/benchmark/async/sql/tpcch/q9.h"

namespace tpcch_sql {

Q9::Q9(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q9::~Q9() {}

transaction_status_t Q9::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT n_name, "
                     "extract(YEAR "
                     "FROM \"order\".entry_d) AS l_year, "
                     "sum(order_line.amount) AS sum_profit "
                     "FROM item, "
                     "stock, "
                     "supplier, "
                     "order_line, "
                     "\"order\", "
                     "nation "
                     "WHERE order_line.i_id = stock.i_id "
                     "AND order_line.supply_w_id = stock.w_id "
                     "AND MOD ((stock.w_id * stock.i_id), 10000) = su_suppkey "
                     "AND order_line.w_id = \"order\".w_id "
                     "AND order_line.d_id = \"order\".d_id "
                     "AND order_line.o_id = \"order\".id "
                     "AND order_line.i_id = item.id "
                     "AND su_nationkey = n_nationkey "
                     "AND item.data LIKE '%bb' "
                     "GROUP BY n_name, "
                     "l_year "
                     "ORDER BY n_name, "
                     "l_year DESC";

    client.Begin(timeout);
    // extract queries are not supported
    //client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}