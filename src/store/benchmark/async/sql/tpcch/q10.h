#ifndef TPCCH_Q10_H
#define TPCCH_Q10_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q10 : public TPCCHSQLTransaction {
 public:
    Q10(uint32_t timeout);
    virtual ~Q10();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
