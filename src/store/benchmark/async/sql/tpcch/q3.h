#ifndef TPCCH_Q3_H
#define TPCCH_Q3_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q3 : public TPCCHSQLTransaction {
 public:
    Q3(uint32_t timeout);
    virtual ~Q3();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
