#ifndef TPCCH_Q12_H
#define TPCCH_Q12_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q12 : public TPCCHSQLTransaction {
 public:
    Q12(uint32_t timeout);
    virtual ~Q12();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
