#ifndef TPCCH_Q6_H
#define TPCCH_Q6_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q6 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q6(uint32_t timeout);
    virtual ~Q6();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
