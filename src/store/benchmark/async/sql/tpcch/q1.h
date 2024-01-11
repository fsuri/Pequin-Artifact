#ifndef TPCCH_Q1_H
#define TPCCH_Q1_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q1 : public TPCCHSQLTransaction {
 public:
    Q1(uint32_t timeout);
    virtual ~Q1();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 