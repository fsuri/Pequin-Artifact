#ifndef TPCCH_Q4_H
#define TPCCH_Q4_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q4 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q4(uint32_t timeout);
    virtual ~Q4();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
