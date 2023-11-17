#include "store/benchmark/async/sql/tpcch/q15.h"

namespace tpcch_sql {

Q15::Q15(uint32_t timeout) : tpcc_sql::TPCCSQLTransaction(timeout) {}

Q15::~Q15() {}

transaction_status_t Q15::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    client.Begin(timeout);
    std::string query = "CREATE view revenue0 (supplier_no, total_revenue) AS "
                    "SELECT "
                    "mod((s_w_id * s_i_id),10000) as supplier_no, "
                    "sum(ol_amount) as total_revenue "
                    "FROM "
                    "order_line, stock "
                    "WHERE "
                    "ol_i_id = s_i_id "
                    "AND ol_supply_w_id = s_w_id "
                    "AND ol_delivery_d >= '2007-01-02 00:00:00.000000' "
                    "GROUP BY "
                    "supplier_no";
    client.SQLRequest(query, queryResult, timeout);
    query = "SELECT su_suppkey, "
                    "su_name, "
                    "su_address, "
                    "su_phone, "
                    "total_revenue "
                    "FROM supplier, revenue0 "
                    "WHERE su_suppkey = supplier_no "
                    "AND total_revenue = (select max(total_revenue) from revenue0) "
                    "ORDER BY su_suppkey";
    client.SQLRequest(query, queryResult, timeout);
    query = "DROP VIEW revenue0";
    client.SQLRequest(query, queryResult, timeout);
    return client.Commit(timeout);
}
}