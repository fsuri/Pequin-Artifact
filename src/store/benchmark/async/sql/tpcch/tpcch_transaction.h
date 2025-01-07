#ifndef TPCCH_SQL_TRANSACTION_H
#define TPCCH_SQL_TRANSACTION_H

#include "store/common/frontend/sync_transaction.h"

namespace tpcch_sql {

class TPCCHSQLTransaction : public SyncTransaction {
    public: 
        TPCCHSQLTransaction(uint32_t timeout);
        virtual ~TPCCHSQLTransaction();
};
}

#endif 