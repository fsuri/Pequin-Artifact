#ifndef TPCCH_Q22_H
#define TPCCH_Q22_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q22 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q22(uint32_t timeout);
    virtual ~Q22();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
