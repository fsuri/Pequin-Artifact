#ifndef TPCCH_Q14_H
#define TPCCH_Q14_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q14 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q14(uint32_t timeout);
    virtual ~Q14();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
