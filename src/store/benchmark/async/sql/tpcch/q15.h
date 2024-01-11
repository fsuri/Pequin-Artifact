#ifndef TPCCH_Q15_H
#define TPCCH_Q15_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q15 : public TPCCHSQLTransaction {
 public:
    Q15(uint32_t timeout);
    virtual ~Q15();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
