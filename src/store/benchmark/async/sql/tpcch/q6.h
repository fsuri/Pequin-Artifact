#ifndef TPCCH_Q6_H
#define TPCCH_Q6_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q6 : public TPCCHSQLTransaction {
 public:
    Q6(uint32_t timeout);
    virtual ~Q6();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
