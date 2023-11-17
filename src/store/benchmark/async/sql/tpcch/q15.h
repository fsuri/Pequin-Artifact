#ifndef TPCCH_Q15_H
#define TPCCH_Q15_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q15 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q15(uint32_t timeout);
    virtual ~Q15();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
