#ifndef TPCCH_Q14_H
#define TPCCH_Q14_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q14 : public TPCCHSQLTransaction {
 public:
    Q14(uint32_t timeout);
    virtual ~Q14();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
