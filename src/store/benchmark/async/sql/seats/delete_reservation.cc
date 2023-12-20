#include "store/benchmark/async/sql/seats/delete_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>
#include <random> 

namespace seats_sql{

SQLDeleteReservation::SQLDeleteReservation(uint32_t timeout, std::mt19937_64 gen, std::queue<SEATSReservation> &existing_res, std::queue<SEATSReservation> &insert_res) : 
    SEATSSQLTransaction(timeout) {
        if (!existing_res.empty()) {
            SEATSReservation r = existing_res.front();
            c_id = r.c_id;
            f_id = r.f_id;
            existing_res.pop();
        } else {
            c_id = std::uniform_int_distribution<int64_t>(1, NUM_CUSTOMERS)(gen);
            f_id = std::uniform_int_distribution<int64_t>(1, NUM_FLIGHTS)(gen);
        }
        if (std::uniform_int_distribution<int>(1, 100)(gen) < PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            c_id_str = std::to_string(c_id);
            c_id = NULL_ID;
        } else {
            c_id_str = "";
        }
        ff_c_id_str = "";

        q = &insert_res;
    }

SQLDeleteReservation::~SQLDeleteReservation() {}


transaction_status_t SQLDeleteReservation::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    Debug("DELETE_RESERVATION");
    client.Begin(timeout);

    query = fmt::format("SELECT f_al_id, f_seats_left, f_iattr00, f_iattr01, f_iattr02, f_iattr03, f_iattr04, f_iattr05, f_iattr06, f_iattr07 FROM {} WHERE f_id = {}", FLIGHT_TABLE, f_id);
    client.Query(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("Flight id %ld not found in Flights Table", f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    GetFlightsResultRow_2 fr_row = GetFlightsResultRow_2();
    deserialize(fr_row, queryResult, 0);
    int64_t ff_al_id = fr_row.f_al_id;
    
    if (c_id == NULL_ID) {
        if (c_id_str.size() > 0) {
            query = fmt::format("SELECT c_id FROM {} WHERE c_id_str = '{}'", CUSTOMER_TABLE, c_id_str);
        } else if (ff_c_id_str.size() > 0 && ff_al_id != NULL_ID) {
            query = fmt::format("SELECT c_id, ff_al_id FROM {}, {} WHERE ff_c_id_str = '{}' AND ff_c_id = c_id", CUSTOMER_TABLE, FREQUENT_FLYER_TABLE, ff_c_id_str);
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
    query = fmt::format("SELECT c_sattr00, c_sattr02, c_sattr04, c_iattr00, c_iattr02, c_iattr04, c_iattr06, f_seats_left, r_id, r_seat, r_price, r_iattr00 FROM {}, {}, {} "
                        "WHERE c_id = {} AND c_id = r_c_id AND f_id = {} AND f_id = r_f_id", CUSTOMER_TABLE, FLIGHT_TABLE, RESERVATION_TABLE, c_id, f_id);
    client.Query(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("No customer record with id %ld", c_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    GetCustomerReservationRow cr_row = GetCustomerReservationRow();
    deserialize(cr_row, queryResult, 0);
    int64_t c_iattr00 = cr_row.c_iattr00;
    int64_t seats_left = cr_row.f_seats_left;
    int64_t r_id = cr_row.r_id;
    int64_t seat = cr_row.r_seat;
    double r_price = cr_row.r_price;
    query = fmt::format("DELETE FROM {} WHERE r_id = {} AND r_c_id = {} AND r_f_id = {}", RESERVATION_TABLE, r_id, c_id, f_id);
    client.Write(query, queryResult, timeout);
    if (!queryResult->has_rows_affected()) {
        Debug("Failed to delete reservation");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    std::mt19937 gen; 
    int requeue = std::uniform_int_distribution<int>(1, 100)(gen);
    if (requeue <= PROB_REQUEUE_DELETED_RESERVATION) 
        q->push(SEATSReservation(NULL_ID, c_id, f_id, seat));

    query = fmt::format("UPDATE {} SET f_seats_left = f_seats_left + 1 WHERE f_id = {}", FLIGHT_TABLE, f_id);
    client.Write(query, queryResult, timeout);
    if (!queryResult->has_rows_affected()) {
        Debug("Failed to update number of seats left in flight");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    query = fmt::format("UPDATE {} SET c_balance = c_balance + {}, c_iattr00 = {}, c_iattr10 = c_iattr10 - 1, c_iattr11 = c_iattr11 - 1 WHERE c_id = {}", CUSTOMER_TABLE, -1 * r_price, c_iattr00, c_id);
    client.Write(query, queryResult, timeout);
    if (!queryResult->has_rows_affected()) {
        Debug("Failed to update customer balance");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    if (ff_al_id != NULL_ID) {
        query = fmt::format("UPDATE {} SET ff_iattr10 = ff_iattr10 - 1 WHERE ff_c_id = {} AND ff_al_id = {}", FREQUENT_FLYER_TABLE, c_id, ff_al_id);
        client.Write(query, queryResult, timeout);
        if (!queryResult->has_rows_affected()) {
            Debug("Failed to update frequent flyer info");
            client.Abort(timeout); 
            return ABORTED_USER;
        }
    }

    return client.Commit(timeout);
}
}