#include "store/benchmark/async/sql/seats/find_open_seats.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include "store/benchmark/async/sql/seats/reservation.h"
#include <fmt/core.h>
#include <queue>


namespace seats_sql {

SQLFindOpenSeats::SQLFindOpenSeats(uint32_t timeout, std::mt19937_64 gen, std::queue<SEATSReservation> &new_res_queue) : 
    SEATSSQLTransaction(timeout) {
        f_id = std::uniform_int_distribution<int64_t>(1, NUM_FLIGHTS)(gen);
        q = &new_res_queue;
    }

SQLFindOpenSeats::~SQLFindOpenSeats() {};

transaction_status_t SQLFindOpenSeats::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    Debug("FIND_OPEN_SEATS on flight %ld", f_id);
    client.Begin(timeout);

    GetFlightResultRow fr_row = GetFlightResultRow();
    //TODO: Does itmake more sense to just Select * and then parse out the fields we want?
    query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, f_base_price, f_seats_total, f_seats_left, "
                        "(f_base_price + (f_base_price * (1 - (f_seats_left / f_seats_total)))) AS f_price FROM {} WHERE f_id = {}", FLIGHT_TABLE, f_id);
    client.Query(query, queryResult, timeout);
    if (queryResult->empty()) { 
        Debug("no flight with that id exists");
        client.Abort(timeout);
        return ABORTED_USER;
    }
    deserialize(fr_row, queryResult, 0);
    double base_price = fr_row.f_base_price;
    int64_t seats_total = fr_row.f_seats_total;
    int64_t seats_left = fr_row.f_seats_left;
    double seat_price = fr_row.f_price;

    double _seat_price = base_price + (base_price * (1.0 - (seats_left/((double)seats_total))));

    query = fmt::format("SELECT r_id, r_f_id, r_seat FROM {} WHERE r_f_id = {}", RESERVATION_TABLE, f_id);
    client.Query(query, queryResult, timeout);

    int8_t unavailable_seats[TOTAL_SEATS_PER_FLIGHT];
    memset(unavailable_seats, 0, TOTAL_SEATS_PER_FLIGHT);

    GetSeatsResultRow sr_row = GetSeatsResultRow();

    for (std::size_t i = 0; i < queryResult->size(); i++) {
        deserialize(sr_row, queryResult, i);
        int seat = (int) sr_row.r_seat;
        unavailable_seats[seat] = (int8_t) 1;
    }

    std::string open_seats_str = "Seats";
    for (int seat = 0; seat < TOTAL_SEATS_PER_FLIGHT; seat++) {
        if (unavailable_seats[seat] == 0) open_seats_str += fmt::format(" {},", seat);
        q->push(SEATSReservation(NULL_ID, NULL_ID, f_id, seat));
    }

    open_seats_str += fmt::format(" are available on flight {} for price %lf", f_id, _seat_price);
    Debug("%s", open_seats_str);
    Debug("COMMIT");

    return client.Commit(timeout);
}
}