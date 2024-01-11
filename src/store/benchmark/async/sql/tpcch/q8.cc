#include "store/benchmark/async/sql/tpcch/q8.h"

namespace tpcch_sql {

Q8::Q8(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q8::~Q8() {}

transaction_status_t Q8::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT extract(YEAR "
                     "FROM \"order\".entry_d) AS l_year, "
                     "sum(CASE WHEN n2.n_name = 'Germany' THEN order_line.amount ELSE 0 END) / sum(order_line.amount) AS mkt_share "
                     "FROM item, "
                     "supplier, "
                     "stock, "
                     "order_line, "
                     "\"order\", "
                     "customer, "
                     "nation n1, "
                     "nation n2, "
                     "region "
                     "WHERE item.id = stock.i_id "
                     "AND order_line.i_id = stock.i_id "
                     "AND order_line.supply_w_id = stock.w_id "
                     "AND MOD ((stock.w_id * stock.i_id), 10000) = su_suppkey "
                     "AND order_line.w_id = \"order\".w_id "
                     "AND order_line.d_id = \"order\".d_id "
                     "AND order_line.o_id = \"order\".id "
                     "AND customer.id = \"order\".c_id "
                     "AND customer.w_id = \"order\".w_id "
                     "AND customer.d_id = \"order\".d_id "
                     "AND n1.n_nationkey = ascii(substring(customer.state from  1  for  1)) "
                     "AND n1.n_regionkey = r_regionkey "
                     "AND order_line.i_id < 1000 "
                     "AND r_name = 'Europe' "
                     "AND su_nationkey = n2.n_nationkey "
                     "AND item.data LIKE '%b' "
                     "AND item.id = order_line.i_id "
                     "GROUP BY l_year "
                     "ORDER BY l_year";

    client.Begin(timeout);
    // No support for extract in postgres?
    //client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}