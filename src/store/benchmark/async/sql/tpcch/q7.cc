#include "store/benchmark/async/sql/tpcch/q7.h"

namespace tpcch_sql {

Q7::Q7(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q7::~Q7() {}

transaction_status_t Q7::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT su_nationkey AS supp_nation, "
                     "substring(customer.state from 1 for 1) AS cust_nation, "
                     "extract(YEAR "
                     "FROM oorder.entry_d) AS l_year, "
                     "sum(order_line.amount) AS revenue "
                     "FROM supplier, "
                     "stock, "
                     "order_line, "
                     "oorder, "
                     "customer, "
                     "nation n1, "
                     "nation n2 "
                     "WHERE order_line.supply_w_id = s_w_id "
                     "AND order_line.i_id = stock.i_id "
                     "AND MOD ((stock.w_id * stock.i_id), 10000) = su_suppkey "
                     "AND order_line.w_id = oorder.w_id "
                     "AND order_line.d_id = oorder.d_id "
                     "AND order_line.o_id = oorder.id "
                     "AND customer.id = oorder.c_id "
                     "AND customer.w_id = oorder.w_id "
                     "AND customer.d_id = oorder.d_id "
                     "AND su_nationkey = n1.n_nationkey "
                     "AND ascii(substring(customer.state from  1  for  1)) = n2.n_nationkey "
                     "AND ((n1.n_name = 'Germany' "
                     "AND n2.n_name = 'Cambodia') "
                     "OR (n1.n_name = 'Cambodia' "
                     "AND n2.n_name = 'Germany')) "
                     "GROUP BY su_nationkey, "
                     "cust_nation, "
                     "l_year "
                     "ORDER BY su_nationkey, "
                     "cust_nation, "
                     "l_year";

    client.Begin(timeout);
    // no support for extract statements
    //client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}