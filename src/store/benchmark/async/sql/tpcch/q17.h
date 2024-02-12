#ifndef TPCCH_Q17_H
#define TPCCH_Q17_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q17 : public TPCCHSQLTransaction {
 public:
    Q17(uint32_t timeout);
    virtual ~Q17();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
