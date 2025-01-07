#ifndef TPCCH_Q21_H
#define TPCCH_Q21_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q21 : public TPCCHSQLTransaction {
 public:
    Q21(uint32_t timeout);
    virtual ~Q21();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
