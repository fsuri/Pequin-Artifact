#ifndef TPCCH_Q13_H
#define TPCCH_Q13_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q13 : public TPCCHSQLTransaction {
 public:
    Q13(uint32_t timeout);
    virtual ~Q13();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
