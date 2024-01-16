#include "store/benchmark/async/sql/tpcch/q5.h"

namespace tpcch_sql {

Q5::Q5(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q5::~Q5() {}

transaction_status_t Q5::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT n_name, "
                     "sum(order_line.amount) AS revenue "
                     "FROM customer, "
                     "oorder, "
                     "order_line, "
                     "stock, "
                     "supplier, "
                     "nation, "
                     "region "
                     "WHERE customer.id = oorder.c_id "
                     "AND customer.w_id = oorder.w_id "
                     "AND customer.d_id = oorder.d_id "
                     "AND order_line.o_id = oorder.id "
                     "AND order_line.w_id = oorder.w_id "
                     "AND order_line.d_id = oorder.d_id "
                     "AND order_line.w_id = stock.w_id "
                     "AND order_line.i_id = stock.i_id "
                     "AND MOD((stock.w_id * stock.i_id), 10000) = su_suppkey "
                     "AND ascii(substring(customer.state from 1 for 1)) = su_nationkey "
                     "AND su_nationkey = n_nationkey "
                     "AND n_regionkey = r_regionkey "
                     "AND r_name = 'Europe' "
                     "AND oorder.entry_d >= 1167714000"  //2007-01-02 00:00:00.000000 in seconds since the epoch (1970) (Using EST here to compare to Local Time Zone which is EST)
                     "GROUP BY n_name "
                     "ORDER BY revenue DESC";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


