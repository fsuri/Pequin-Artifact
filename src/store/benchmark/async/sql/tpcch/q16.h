#ifndef TPCCH_Q16_H
#define TPCCH_Q16_H

#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

class Q16 : public TPCCHSQLTransaction {
 public:
    Q16(uint32_t timeout);
    virtual ~Q16();
    virtual transaction_status_t Execute(SyncClient &client);
};

}

#endif 
