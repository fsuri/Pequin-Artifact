#include "store/benchmark/async/sql/tpcch/q22.h"

namespace tpcch_sql {

Q22::Q22(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q22::~Q22() {}

transaction_status_t Q22::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT substring(customer.state from 1 for 1) AS country, "
                     "count(*) AS numcust, "
                     "sum(customer.balance) AS totacctbal "
                     "FROM customer "
                     "WHERE substring(customer.phone from 1 for 1) IN ('1', "
                     "'2', "
                     "'3', "
                     "'4', "
                     "'5', "
                     "'6', "
                     "'7') "
                     "AND customer.balance > "
                     "(SELECT avg(customer.balance) "
                     "FROM customer "
                     "WHERE customer.balance > 0.00 "
                     "AND substring(customer.phone from 1 for 1) IN ('1', "
                     "'2', "
                     "'3', "
                     "'4', "
                     "'5', "
                     "'6', "
                     "'7')) "
                     "AND NOT EXISTS "
                     "(SELECT * "
                     "FROM \"order\" "
                     "WHERE \"order\".c_id = customer.id "
                     "AND \"order\".w_id = customer.w_id "
                     "AND \"order\".d_id = customer.d_id) "
                     "GROUP BY substring(customer.state from 1 for 1) "
                     "ORDER BY substring(customer.state,1,1)";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}