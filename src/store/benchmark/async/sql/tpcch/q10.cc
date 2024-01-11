#include "store/benchmark/async/sql/tpcch/q10.h"

namespace tpcch_sql {

Q10::Q10(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q10::~Q10() {}

transaction_status_t Q10::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT customer.id, "
                     "customer.last, "
                     "sum(order_line.amount) AS revenue, "
                     "customer.city, "
                     "customer.phone, "
                     "n_name "
                     "FROM customer, "
                     "\"order\", "
                     "order_line, "
                     "nation "
                     "WHERE customer.id = \"order\".c_id "
                     "AND customer.w_id = \"order\".w_id "
                     "AND customer.d_id = \"order\".d_id "
                     "AND order_line.w_id = \"order\".w_id "
                     "AND order_line.d_id = \"order\".d_id "
                     "AND order_line.o_id = \"order\".id "
                     "AND \"order\".entry_d >= 1167714000"
                     "AND \"order\".entry_d <= order_line.delivery_d "
                     "AND n_nationkey = ascii(substring(customer.state from  1  for  1)) "
                     "GROUP BY customer.id, "
                     "customer.last, "
                     "customer.city, "
                     "customer.phone, "
                     "n_name "
                     "ORDER BY revenue DESC";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}