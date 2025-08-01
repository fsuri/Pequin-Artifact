#include "store/benchmark/async/sql/tpcch/q1.h"
#include "store/benchmark/async/sql/tpcch/tpcch_constants.h"
using namespace tpcc_sql;

namespace tpcch_sql {

Q1::Q1(uint32_t timeout) : TPCCHSQLTransaction(timeout) {}

Q1::~Q1() {}

transaction_status_t Q1::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;

    int delivery_date = 1167714000; //2007-01-02 00:00:00.000000 in seconds since the epoch (1970) (Using EST here to compare to Local Time Zone which is EST)

    std::string query = "SELECT ol_number, " 
                    "sum(ol_quantity) AS sum_qty, "
                    "sum(ol_amount) AS sum_amount, "
                    "avg(ol_quantity) AS avg_qty, "
                    "avg(ol_amount) AS avg_amount, "
                    "count(*) AS count_order "
                    "FROM order_line " 
                    "WHERE ol_delivery_d > " + std::to_string(delivery_date) +
                    "GROUP BY ol_number "
                    "ORDER BY ol_number";

    client.Begin(timeout);
    client.Query(query, queryResult, timeout);
    std::cerr << "Done" << std::endl;
    return client.Commit(timeout);
}
}


