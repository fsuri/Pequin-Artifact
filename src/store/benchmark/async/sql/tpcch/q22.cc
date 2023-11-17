#include "store/benchmark/async/sql/tpcch/q22.h"

namespace tpcch_sql {

Q22::Q22(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q22::~Q22() {}

transaction_status_t Q22::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT substring(c_state from 1 for 1) AS country, "
                     "count(*) AS numcust, "
                     "sum(c_balance) AS totacctbal "
                     "FROM customer "
                     "WHERE substring(c_phone from 1 for 1) IN ('1', "
                     "'2', "
                     "'3', "
                     "'4', "
                     "'5', "
                     "'6', "
                     "'7') "
                     "AND c_balance > "
                     "(SELECT avg(c_balance) "
                     "FROM customer "
                     "WHERE c_balance > 0.00 "
                     "AND substring(c_phone from 1 for 1) IN ('1', "
                     "'2', "
                     "'3', "
                     "'4', "
                     "'5', "
                     "'6', "
                     "'7')) "
                     "AND NOT EXISTS "
                     "(SELECT * "
                     "FROM oorder "
                     "WHERE o_c_id = c_id "
                     "AND o_w_id = c_w_id "
                     "AND o_d_id = c_d_id) "
                     "GROUP BY substring(c_state from 1 for 1) "
                     "ORDER BY substring(c_state,1,1)";

    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}