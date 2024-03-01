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

struct GetCustomerResultRow {
public:
    GetCustomerResultRow() {}
    ~GetCustomerResultRow() {}
    int64_t c_id;
    std::string c_id_str;
    int64_t c_base_ap_id;
    double c_balance;
    std::string c_sattr00;
    std::string c_sattr01;
    std::string c_sattr02;
    std::string c_sattr03;
    std::string c_sattr04;
    std::string c_sattr05;
    std::string c_sattr06;
    std::string c_sattr07;
    std::string c_sattr08;
    std::string c_sattr09;
    std::string c_sattr10;
    std::string c_sattr11;
    std::string c_sattr12;
    std::string c_sattr13;
    std::string c_sattr14;
    std::string c_sattr15;
    std::string c_sattr16;
    std::string c_sattr17;
    std::string c_sattr18;
    std::string c_sattr19;
    int64_t c_iattr00;
    int64_t c_iattr01;
    int64_t c_iattr02;
    int64_t c_iattr03;
    int64_t c_iattr04;
    int64_t c_iattr05;
    int64_t c_iattr06;
    int64_t c_iattr07;
    int64_t c_iattr08;
    int64_t c_iattr09;
    int64_t c_iattr10;
    int64_t c_iattr11;
    int64_t c_iattr12;
    int64_t c_iattr13;
    int64_t c_iattr14;
    int64_t c_iattr15;
    int64_t c_iattr16;
    int64_t c_iattr17;
    int64_t c_iattr18;
    int64_t c_iattr19;
};

void inline load_row(GetCustomerResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.c_id);
    row->get(1, &store.c_id_str);
    row->get(2, &store.c_base_ap_id);
    row->get(3, &store.c_balance);
    row->get(4, &store.c_sattr00);
    row->get(5, &store.c_sattr01);
    row->get(6, &store.c_sattr02);
    row->get(7, &store.c_sattr03);
    row->get(8, &store.c_sattr04);
    row->get(9, &store.c_sattr05);
    row->get(10, &store.c_sattr06);
    row->get(11, &store.c_sattr07);
    row->get(12, &store.c_sattr08);
    row->get(13, &store.c_sattr09);
    row->get(14, &store.c_sattr10);
    row->get(15, &store.c_sattr11);
    row->get(16, &store.c_sattr12);
    row->get(17, &store.c_sattr13);
    row->get(18, &store.c_sattr14);
    row->get(19, &store.c_sattr15);
    row->get(20, &store.c_sattr16);
    row->get(21, &store.c_sattr17);
    row->get(22, &store.c_sattr18);
    row->get(23, &store.c_sattr19);
    row->get(24, &store.c_iattr00);
    row->get(25, &store.c_iattr01);
    row->get(26, &store.c_iattr02);
    row->get(27, &store.c_iattr03);
    row->get(28, &store.c_iattr04);
    row->get(29, &store.c_iattr05);
    row->get(30, &store.c_iattr06);
    row->get(31, &store.c_iattr07);
    row->get(32, &store.c_iattr08);
    row->get(33, &store.c_iattr09);
    row->get(34, &store.c_iattr10);
    row->get(35, &store.c_iattr11);
    row->get(36, &store.c_iattr12);
    row->get(37, &store.c_iattr13);
    row->get(38, &store.c_iattr14);
    row->get(39, &store.c_iattr15);
    row->get(40, &store.c_iattr16);
    row->get(41, &store.c_iattr17);
    row->get(42, &store.c_iattr18);
    row->get(43, &store.c_iattr19);
}

struct GetFrequentFlyersResultRow {
    GetFrequentFlyersResultRow() {}
    ~GetFrequentFlyersResultRow() {}
    int64_t ff_c_id;
    int64_t ff_al_id;
    std::string ff_c_id_str;
    std::string ff_sattr00;
    std::string ff_sattr01;
    std::string ff_sattr02;
    std::string ff_sattr03;
    int64_t ff_iattr00;
    int64_t ff_iattr01;
    int64_t ff_iattr02;
    int64_t ff_iattr03;
    int64_t ff_iattr04;
    int64_t ff_iattr05;
    int64_t ff_iattr06;
    int64_t ff_iattr07;
    int64_t ff_iattr08;
    int64_t ff_iattr09;
    int64_t ff_iattr10;
    int64_t ff_iattr11;
    int64_t ff_iattr12;
    int64_t ff_iattr13;
    int64_t ff_iattr14;
    int64_t ff_iattr15;
};

void inline load_row(GetFrequentFlyersResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.ff_c_id);
    row->get(1, &store.ff_al_id);
    row->get(2, &store.ff_c_id_str);
    row->get(3, &store.ff_sattr00);
    row->get(4, &store.ff_sattr01);
    row->get(5, &store.ff_sattr02);
    row->get(6, &store.ff_sattr03);
    row->get(7, &store.ff_iattr00);
    row->get(8, &store.ff_iattr01);
    row->get(9, &store.ff_iattr02);
    row->get(10, &store.ff_iattr03);
    row->get(11, &store.ff_iattr04);
    row->get(12, &store.ff_iattr05);
    row->get(13, &store.ff_iattr06);
    row->get(14, &store.ff_iattr07);
    row->get(15, &store.ff_iattr08);
    row->get(16, &store.ff_iattr09);
    row->get(17, &store.ff_iattr10);
    row->get(18, &store.ff_iattr11);
    row->get(19, &store.ff_iattr12);
    row->get(20, &store.ff_iattr13);
    row->get(21, &store.ff_iattr14);
    row->get(22, &store.ff_iattr15);
}

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