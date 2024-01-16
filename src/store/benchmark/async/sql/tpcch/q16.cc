#include "store/benchmark/async/sql/tpcch/q16.h"

namespace tpcch_sql {

Q16::Q16(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q16::~Q16() {}

transaction_status_t Q16::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query = "SELECT item.name, "
                     "substring(item.data from  1 for 3) AS brand, "
                     "item.price, "
                     "count(DISTINCT (mod((stock.w_id * stock.i_id),10000))) AS supplier_cnt "
                     "FROM stock, "
                     "item "
                     "WHERE item.id = stock.i_id "
                     "AND item.data NOT LIKE 'zz%' "
                     "AND (mod((stock.w_id * stock.i_id),10000) NOT IN "
                     "(SELECT su_suppkey "
                     "FROM supplier "
                     "WHERE su_comment LIKE '%bad%')) "
                     "GROUP BY item.name, "
                     "brand, "
                     "item.price "
                     "ORDER BY supplier_cnt DESC";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}