#ifndef TPCCH_Q11_H
#define TPCCH_Q11_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q11 : public TPCCHSQLTransaction {
 public:
    Q11(uint32_t timeout);
    virtual ~Q11();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
