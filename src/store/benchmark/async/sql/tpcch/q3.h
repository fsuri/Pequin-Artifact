#ifndef TPCCH_Q3_H
#define TPCCH_Q3_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q3 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q3(uint32_t timeout);
    virtual ~Q3();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
