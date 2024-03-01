#include "store/benchmark/async/sql/tpcch/tpcch_transaction.h"

namespace tpcch_sql {

TPCCHSQLTransaction::TPCCHSQLTransaction(uint32_t timeout) : SyncTransaction(timeout) {
}

TPCCHSQLTransaction::~TPCCHSQLTransaction() {
}

}
