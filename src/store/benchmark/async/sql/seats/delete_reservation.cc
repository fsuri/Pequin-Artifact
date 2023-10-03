#include "store/benchmark/async/sql/seats/delete_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>

namespace seats_sql{

SQLDeleteReservation::SQLDeleteReservation(uint32_t timeout, int64_t f_id, int64_t c_id, std::string c_id_str, std::string ff_c_id_str) : 
    SEATSSQLTransaction(timeout), f_id(f_id), c_id(c_id), c_id_str(c_id_str), ff_c_id_str(ff_c_id_str) {}

SQLDeleteReservation::~SQLDeleteReservation() {}

struct GetFlightsResultRow {
public: 
    GetFlightsResultRow() : f_al_id(0), f_seats_left(0), f_iattr00(0), f_iattr01(0), f_iattr02(0), f_iattr03(0), f_iattr04(0), f_iattr05(0), f_iattr06(0), f_iattr07(0) {}
    virtual ~GetFlightsResultRow() {} 
    virtual void SetFields(int64_t f_al_id, int64_t f_seats_left, int64_t f_iattr00, int64_t f_iattr01, int64_t f_iattr02, int64_t f_iattr03, int64_t f_iattr04, int64_t f_iattr05, int64_t f_iattr06, int64_t f_iattr07) {
        f_al_id = f_al_id; 
        f_seats_left = f_seats_left;
        f_iattr00 = f_iattr00;
        f_iattr01 = f_iattr01;
        f_iattr02 = f_iattr02;
        f_iattr03 = f_iattr03;
        f_iattr04 = f_iattr04;
        f_iattr05 = f_iattr05; 
        f_iattr06 = f_iattr06; 
        f_iattr07 = f_iattr07;
    }
    int64_t f_al_id;
    int64_t f_seats_left;
    int64_t f_iattr00;
    int64_t f_iattr01;
    int64_t f_iattr02;
    int64_t f_iattr03;
    int64_t f_iattr04;
    int64_t f_iattr05;
    int64_t f_iattr06;
    int64_t f_iattr07;
};

void inline load_row(GetFlightsResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.f_al_id);
    row->get(1, &store.f_seats_left);
    row->get(2, &store.f_iattr00);
    row->get(3, &store.f_iattr01);
    row->get(4, &store.f_iattr02);
    row->get(5, &store.f_iattr03); 
    row->get(6, &store.f_iattr04);
    row->get(7, &store.f_iattr05);
    row->get(8, &store.f_iattr06);
    row->get(9, &store.f_iattr07);
}

struct GetCustomerReservationRow {
public: 
    GetCustomerReservationRow() : c_sattr00(""), c_sattr02(""), c_sattr04(""), 
        c_iattr00(0), c_iattr02(0), c_iattr04(0), c_iattr06(0), f_seats_left(0), r_id(0), r_seat(0), r_price(0), r_iattr00(0) {}
    virtual ~GetCustomerReservationRow() {}
    std::string c_sattr00;
    std::string c_sattr02;
    std::string c_sattr04;
    int64_t c_iattr00;
    int64_t c_iattr02;
    int64_t c_iattr04;
    int64_t c_iattr06;
    int64_t f_seats_left;
    int64_t r_id;
    int64_t r_seat;
    int64_t r_price;
    int64_t r_iattr00;
};

void inline load_row(GetCustomerReservationRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.c_sattr00);
    row->get(1, &store.c_sattr02);
    row->get(2, &store.c_sattr04);
    row->get(3, &store.c_iattr00);
    row->get(4, &store.c_iattr02);
    row->get(5, &store.c_iattr04); 
    row->get(6, &store.c_iattr06);
    row->get(7, &store.f_seats_left);
    row->get(8, &store.r_id);
    row->get(9, &store.r_seat);
    row->get(10, &store.r_price);
    row->get(11, &store.r_iattr00);
}

transaction_status_t SQLDeleteReservation::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    Debug("DELETE_RESERVATION");
    client.Begin(timeout);

    query = fmt::format("SELECT F_AL_ID, F_SEATS_LEFT, F_IATTR00, F_IATTR01, F_IATTR02, F_IATTR03, F_IATTR04, F_IATTR05, F_IATTR06, F_IATTR07 FROM {} WHERE F_ID = {}", FLIGHT_TABLE, f_id);
    client.Query(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("Flight id %d not found in Flights Table", f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    GetFlightsResultRow fr_row = GetFlightsResultRow();
    deserialize(fr_row, queryResult, 0);
    int64_t ff_al_id = fr_row.f_al_id;
    
    if (c_id == NULL_ID) {
        if (c_id_str.size() > 0) {
            query = fmt::format("SELECT C_ID FROM {} WHERE C_ID_STR = {}", CUSTOMER_TABLE, c_id_str);
        } else if (ff_c_id_str.size() > 0 && ff_al_id != NULL_ID) {
            query = fmt::format("SELECT C_ID, FF_AL_ID FROM {}, {} WHERE FF_C_ID_STR = {} AND FF_C_ID = C_ID", CUSTOMER_TABLE, FREQUENT_FLYER_TABLE, ff_c_id_str);
        } else {
            Debug("No way to get Customer ID");
            client.Abort(timeout);
            return ABORTED_USER;
        }
        client.Query(query, queryResult, timeout);
        if (queryResult->empty()) {
            Debug("No customer record found");
            client.Abort(timeout);
            return ABORTED_USER;
        }
        deserialize(c_id, queryResult, 0);
    }
    query = fmt::format("SELECT C_SATTR00, C_SATTR02, C_SATTR04, C_IATTR00, C_IATTR02, C_IATTR04, C_IATTR06, F_SEATS_LEFT, R_ID, R_SEAT, R_PRICE, R_IATTR00 FROM {}, {}, {} WHERE C_ID = {} AND C_ID = R_C_ID AND F_ID = {} AND F_ID = R_F_ID", CUSTOMER_TABLE, FLIGHT_TABLE, RESERVATION_TABLE, c_id, f_id);
    client.Query(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("No customer record with id %s", c_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    GetCustomerReservationRow cr_row = GetCustomerReservationRow();
    deserialize(cr_row, queryResult, 0);
    int64_t c_iattr00 = cr_row.c_iattr00;
    int64_t seats_left = cr_row.f_seats_left;
    int64_t r_id = cr_row.r_id;
    float r_price = cr_row.r_price;
    query = fmt::format("DELETE FROM {} WHERE R_ID = {} AND R_C_ID = {} AND R_F_ID = {}", RESERVATION_TABLE, r_id, c_id, f_id);
    client.Write(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("Failed to delete reservation");
        client.Abort(timeout);
        return ABORTED_USER;
    }
    query = fmt::format("UPDATE {} SET F_SEATS_LEFT = F_SEATS_LEFT + 1 WHERE F_ID = {}", FLIGHT_TABLE, f_id);
    client.Write(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("Failed to update number of seats left in flight");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    query = fmt::format("UPDATE {} SET C_BALANCE = C_BALANCE + {}, C_IATTR0 = {}, C_IATTR10 = C_IATTR10 - 1, C_IATTR11 = C_IATTR10 - 1 WHERE C_ID = {}", CUSTOMER_TABLE, -1 * r_price, c_iattr00, c_id);
    client.Write(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("Failed to update customer balance");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    if (ff_al_id != NULL_ID) {
        query = fmt::format("UPDATE {} SET FF_IATTR10 = FF_IATTR10 - 1 WHERE FF_C_ID = {} AND FF_AL_ID = {}", FREQUENT_FLYER_TABLE, c_id, ff_al_id);
        client.Write(query, queryResult, timeout);
        if (queryResult->empty()) {
            Debug("Failed to update frequent flyer info");
            client.Abort(timeout); 
            return ABORTED_USER;
        }
    }

    return client.Commit(timeout);
}
}