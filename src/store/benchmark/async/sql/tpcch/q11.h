#ifndef TPCCH_Q11_H
#define TPCCH_Q11_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q11 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q11(uint32_t timeout);
    virtual ~Q11();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
