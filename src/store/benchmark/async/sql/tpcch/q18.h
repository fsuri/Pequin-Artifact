#ifndef TPCCH_Q18_H
#define TPCCH_Q18_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q18 : public TPCCHSQLTransaction {
 public:
    Q18(uint32_t timeout);
    virtual ~Q18();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
