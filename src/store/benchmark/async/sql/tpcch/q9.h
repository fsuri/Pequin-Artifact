#ifndef TPCCH_Q9_H
#define TPCCH_Q9_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q9 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q9(uint32_t timeout);
    virtual ~Q9();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
