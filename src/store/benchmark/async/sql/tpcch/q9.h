#ifndef TPCCH_Q9_H
#define TPCCH_Q9_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q9 : public TPCCHSQLTransaction {
 public:
    Q9(uint32_t timeout);
    virtual ~Q9();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
