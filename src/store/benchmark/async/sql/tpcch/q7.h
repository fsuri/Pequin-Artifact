#ifndef TPCCH_Q7_H
#define TPCCH_Q7_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q7 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q7(uint32_t timeout);
    virtual ~Q7();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
