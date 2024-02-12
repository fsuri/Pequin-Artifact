#ifndef TPCCH_Q20_H
#define TPCCH_Q20_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q20 : public TPCCHSQLTransaction {
 public:
    Q20(uint32_t timeout);
    virtual ~Q20();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
