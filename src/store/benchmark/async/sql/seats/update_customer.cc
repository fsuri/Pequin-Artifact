#include "store/benchmark/async/sql/seats/update_customer.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>

namespace seats_sql {

SQLUpdateCustomer::SQLUpdateCustomer(uint32_t timeout, std::mt19937_64 gen) 
    : SEATSSQLTransaction(timeout) {
        c_id = std::uniform_int_distribution<int64_t>(1, NUM_CUSTOMERS)(gen);
        if (std::uniform_int_distribution<int>(1, 100)(gen) < PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            c_id_str = std::to_string(c_id);
            c_id = NULL_ID;
        }
        if (std::uniform_int_distribution<int>(1, 100)(gen) < PROB_UPDATE_FREQUENT_FLYER) {
            update_ff = 1;
        } else update_ff = 0;
        attr0 = std::uniform_int_distribution<int64_t>(1, 100000)(gen);
        attr1 = std::uniform_int_distribution<int64_t>(1, 100000)(gen);
    }

SQLUpdateCustomer::~SQLUpdateCustomer() {}

transaction_status_t SQLUpdateCustomer::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;
    Debug("UPDATE_CUSTOMER");
    client.Begin(timeout);

    if (c_id == NULL_ID) {
        if (c_id_str.size() == 0) Panic("no customer id nor customer id string given");
        query = fmt::format("SELECT c_id FROM {} WHERE c_id_str = '{}'", CUSTOMER_TABLE, c_id_str);
        client.Query(query, queryResult, timeout);
        if (queryResult->empty()) {
            Debug("No customer record found for customer id string %s", c_id_str);
            client.Abort(timeout);
            return ABORTED_USER;
        }
        deserialize(c_id, queryResult, 0);
    }
    query = fmt::format("SELECT * FROM {} WHERE c_id = {}", CUSTOMER_TABLE, c_id);
    client.Query(query, queryResult, timeout);
    GetCustomerResultRow cr_row = GetCustomerResultRow();
    if (queryResult->empty()) {
        Debug("No customer record for customr id %ld", c_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    deserialize(cr_row, queryResult, 0);
    int64_t base_airport = cr_row.c_base_ap_id;
    query = fmt::format("SELECT * FROM {}, {} WHERE ap_id = {} AND ap_co_id = co_id", AIRPORT_TABLE, COUNTRY_TABLE, base_airport);
    client.Query(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("No airport found");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    // update frequent flyers
    if (update_ff > 0) {
        query = fmt::format("SELECT * FROM {} WHERE ff_c_id = {}", FREQUENT_FLYER_TABLE, c_id);
        client.Query(query, queryResult, timeout);
        GetFrequentFlyersResultRow ffr_row = GetFrequentFlyersResultRow();
        std::unique_ptr<const query_result::QueryResult> queryResult2;
        for (std::size_t i = 0; i < queryResult->size(); i++) {
            deserialize(ffr_row, queryResult, (int) i);
            int64_t ff_al_id = ffr_row.ff_al_id;
            query = fmt::format("UPDATE {} SET ff_iattr00 = {}, ff_iattr01 = {} WHERE ff_c_id = {} AND ff_al_id = {}", FREQUENT_FLYER_TABLE, attr0, attr1, c_id, ff_al_id);
            client.Write(query, queryResult2, timeout);
        }
    }

    query = fmt::format("UPDATE {} SET c_iattr00 = {}, c_iattr01 = {} WHERE c_id = {}", CUSTOMER_TABLE, attr0, attr1, c_id);
    client.Write(query, queryResult, timeout);
    if (!queryResult->has_rows_affected()) {
        Debug("Update Customer failed");
        client.Abort(timeout);
        return ABORTED_USER;
    }
    
    return client.Commit(timeout);
}

}