#ifndef TPCCH_Q19_H
#define TPCCH_Q19_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q19 : public TPCCHSQLTransaction {
 public:
    Q19(uint32_t timeout);
    virtual ~Q19();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
