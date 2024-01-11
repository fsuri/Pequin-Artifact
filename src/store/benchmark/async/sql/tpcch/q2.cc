#include "store/benchmark/async/sql/tpcch/q2.h"

namespace tpcch_sql {

Q2::Q2(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q2::~Q2() {}

transaction_status_t Q2::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query =             "SELECT su_suppkey, su_name, "
                    "n_name, "
                    "item.id, "
                    "item.name, "
                    "su_address, "
                    "su_phone, "
                    "su_comment "
                    "FROM item, supplier, stock, nation, region, "
                    "(SELECT stock.i_id AS m_i_id, MIN(stock.quantity) AS m_s_quantity "
                    "FROM stock, "
                    "supplier, "
                    "nation, "
                    "region "
                    "WHERE MOD((stock.w_id*stock.i_id), 10000)=su_suppkey "
                    "AND su_nationkey=n_nationkey "
                    "AND n_regionkey=r_regionkey "
                    "AND r_name LIKE 'Europ%' "
                    "GROUP BY stock.i_id) m "
                    "WHERE item.id = stock.i_id "
                    "AND MOD((stock.w_id * stock.i_id), 10000) = su_suppkey "
                    "AND su_nationkey = n_nationkey "
                    "AND n_regionkey = r_regionkey "
                    "AND item.data LIKE '%b' "
                    "AND r_name LIKE 'Europ%' "
                    "AND item.id=m_i_id "
                    "AND stock.quantity = m_s_quantity "
                    "ORDER BY n_name, "
                    "su_name, "
                    "item.id";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    return client.Commit(timeout);
}
}


