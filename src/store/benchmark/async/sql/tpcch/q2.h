#ifndef TPCCH_Q2_H
#define TPCCH_Q2_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q2 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q2(uint32_t timeout);
    virtual ~Q2();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
