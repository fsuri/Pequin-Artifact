#include "store/benchmark/async/sql/tpcch/q16.h"

namespace tpcch_sql {

Q16::Q16(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q16::~Q16() {}

transaction_status_t Q16::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT i_name, "
                     "substring(i_data from  1 for 3) AS brand, "
                     "i_price, "
                     "count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt "
                     "FROM stock, "
                     "item "
                     "WHERE i_id = s_i_id "
                     "AND i_data NOT LIKE 'zz%' "
                     "AND (mod((s_w_id * s_i_id),10000) NOT IN "
                     "(SELECT su_suppkey "
                     "FROM supplier "
                     "WHERE su_comment LIKE '%bad%')) "
                     "GROUP BY i_name, "
                     "brand, "
                     "i_price "
                     "ORDER BY supplier_cnt DESC";

    client.Begin(timeout);
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}