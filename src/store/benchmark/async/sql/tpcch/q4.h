#ifndef TPCCH_Q4_H
#define TPCCH_Q4_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q4 : public TPCCHSQLTransaction {
 public:
    Q4(uint32_t timeout);
    virtual ~Q4();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
