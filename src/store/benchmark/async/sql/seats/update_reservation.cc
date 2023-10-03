#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>

namespace seats_sql {
SQLUpdateReservation::SQLUpdateReservation(uint32_t timeout, int64_t r_id, int64_t c_id, int64_t f_id, int64_t seatnum, int64_t attr_idx, int64_t attr_val)
    : SEATSSQLTransaction(timeout), r_id(r_id), c_id(c_id), f_id(f_id), seatnum(seatnum), attr_idx(attr_idx), attr_val(attr_val) {}

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

