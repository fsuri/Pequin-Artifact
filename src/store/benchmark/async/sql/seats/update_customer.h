#ifndef SEATS_SQL_UPDATE_CUSTOMER_H
#define SEATS_SQL_UPDATE_CUSTOMER_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"

namespace seats_sql {

class SQLUpdateCustomer:public SEATSSQLTransaction {
    public: 
        SQLUpdateCustomer(uint32_t timeout, std::mt19937_64 gen);
        virtual ~SQLUpdateCustomer();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t c_id;
        std::string c_id_str;
        int64_t update_ff;
        int64_t attr0; 
        int64_t attr1;
};

}

#endif

