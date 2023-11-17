#ifndef TPCCH_Q8_H
#define TPCCH_Q8_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcch_sql {

class Q8 : public tpcc_sql::TPCCSQLTransaction {
 public:
    Q8(uint32_t timeout);
    virtual ~Q8();
    virtual transaction_status_t Execute(SyncClient &client);

};

}

#endif 
