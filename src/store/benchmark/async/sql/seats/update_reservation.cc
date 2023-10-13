#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>

namespace seats_sql {
SQLUpdateReservation::SQLUpdateReservation(uint32_t timeout, std::mt19937_64 gen)
    : SEATSSQLTransaction(timeout) {
        c_id = std::uniform_int_distribution<int64_t>(1, NUM_CUSTOMERS)(gen);
        // figure out how to get customer -- reserve id mapping in client
        r_id = std::uniform_int_distribution<int64_t>(1, 1000000)(gen);
        f_id = std::uniform_int_distribution<int64_t>(1, NUM_FLIGHTS)(gen);
        seatnum = std::uniform_int_distribution<int64_t>(1, TOTAL_SEATS_PER_FLIGHT)(gen);
        attr_idx = std::uniform_int_distribution<int64_t>(0, 3)(gen);
        attr_val = std::uniform_int_distribution<int64_t>(1, 100000)(gen);
    }

SQLUpdateReservation::~SQLUpdateReservation() {}

transaction_status_t SQLUpdateReservation::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::unique_ptr<const query_result::QueryResult> queryResult2;
    std::string query;
    Debug("UPDATE_RESERVATION");
    client.Begin(timeout);

    query = fmt::format("SELECT R_ID FROM {} WHERE R_F_ID = {} AND R_SEAT = {}", RESERVATION_TABLE, f_id, seatnum);
    client.Query(query, queryResult, timeout);
    query = fmt::format("SELECT R_ID FROM {} WHERE R_F_ID = {} AND R_C_ID = {}", RESERVATION_TABLE, f_id, c_id);
    client.Query(query, queryResult2, timeout);

    if (!queryResult->empty()) {
        Debug("Seat %d is already reserved on flight %d!", seatnum, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    if (queryResult2->empty()) {
        Debug("Customer %d does not have an existing reservation flight %d", c_id, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    std::time_t update_time = std::time(nullptr);

    query = fmt::format("UPDATE {} SET R_SEAT = {}, R_UPDATED = {}, {} = {} WHERE R_ID = {} AND R_C_ID = {} AND R_F_ID = {}", RESERVATION_TABLE, seatnum, (int64_t) update_time, reserve_seats[attr_idx], attr_val, r_id, c_id, f_id);
    client.Write(query, queryResult, timeout);
    if (queryResult->empty()) {
        Debug("Failed to update reservation");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    return client.Commit(timeout);
}
}

