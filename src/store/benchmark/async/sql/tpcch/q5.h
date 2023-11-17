#ifndef TPCCH_Q5_H
#define TPCCH_Q5_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q5 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q5(uint32_t timeout);
    virtual ~Q5();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
