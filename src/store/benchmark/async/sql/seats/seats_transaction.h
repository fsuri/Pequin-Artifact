#ifndef SEATS_SQL_TRANSACTION_H
#define SEATS_SQL_TRANSACTION_H

#include "store/common/frontend/sync_client.h"
#include "store/common/frontend/sync_transaction.h"
#include "lib/cereal/archives/binary.hpp"
#include "lib/cereal/types/string.hpp"

namespace seats_sql {

template <class T>
void load_row(T& t, std::unique_ptr<query_result::Row> row) {
  row->get(0, &t);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row) {
  load_row(t, queryResult->at(row));
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult) {
  deserialize(t, queryResult, 0);
}

class SEATSSQLTransaction : public SyncTransaction {
    public: 
        SEATSSQLTransaction(uint32_t timeout);
        virtual ~SEATSSQLTransaction();
};
}

#endif 