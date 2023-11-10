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

struct GetFlightResultRow {
public: 
    GetFlightResultRow() : f_id(0), f_al_id(0), f_depart_ap_id(0), f_depart_time(0), f_arrive_ap_id(0), f_arrive_time(0), f_base_price(0.0), f_seats_total(0), f_price(0.0) {}
    ~GetFlightResultRow() {}
    int64_t f_id;
    int64_t f_al_id; 
    int64_t f_depart_ap_id; 
    int64_t f_depart_time; 
    int64_t f_arrive_ap_id; 
    int64_t f_arrive_time;
    double f_base_price; 
    int64_t f_seats_total; 
    int64_t f_seats_left;
    double f_price;
};

void inline load_row(GetFlightResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.f_id);
    row->get(1, &store.f_al_id);
    row->get(2, &store.f_depart_ap_id);
    row->get(3, &store.f_depart_time);
    row->get(4, &store.f_arrive_ap_id);
    row->get(5, &store.f_arrive_time);
    row->get(6, &store.f_base_price);
    row->get(7, &store.f_seats_total);
    row->get(8, &store.f_seats_left);
    row->get(9, &store.f_price);
}

struct GetSeatsResultRow {
public: 
    GetSeatsResultRow() : r_id(0), r_f_id(0), r_seat(0) {}
    ~GetSeatsResultRow() {} 
    int64_t r_id;
    int64_t r_f_id;
    int64_t r_seat;
};

void inline load_row(GetSeatsResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.r_id);
    row->get(1, &store.r_f_id);
    row->get(2, &store.r_seat);
}

transaction_status_t SQLFindOpenSeats::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    Debug("FIND_OPEN_SEATS on flight %ld", f_id);
    std::cerr << f_id << std::endl;
    client.Begin(timeout);

    GetFlightResultRow fr_row = GetFlightResultRow();
    query = fmt::format("SELECT F_ID, F_AL_ID, F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, F_BASE_PRICE, F_SEATS_TOTAL, F_SEATS_LEFT, (F_BASE_PRICE + (F_BASE_PRICE * (1 - (F_SEATS_LEFT / F_SEATS_TOTAL)))) AS F_PRICE FROM {} WHERE F_ID = {}", FLIGHT_TABLE, f_id);
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
    query = fmt::format("SELECT R_ID, R_F_ID, R_SEAT FROM {} WHERE R_F_ID = {}", RESERVATION_TABLE, f_id);
    client.Query(query, queryResult, timeout);

    int8_t unavailable_seats[TOTAL_SEATS_PER_FLIGHT];
    memset(unavailable_seats, 0, TOTAL_SEATS_PER_FLIGHT);

    GetSeatsResultRow sr_row = GetSeatsResultRow();

    for (int i = 0; i < queryResult->size(); i++) {
        deserialize(sr_row, queryResult, i);
        int seat = (int) sr_row.r_seat;
        unavailable_seats[seat] = (int8_t) 1;
    }

    std::string open_seats_str = "Seats";
    for (int seat = 0; seat < TOTAL_SEATS_PER_FLIGHT; seat++) {
        if (unavailable_seats[seat] == 0) open_seats_str += fmt::format(" {},", seat);
        q->push(SEATSReservation(NULL_ID, NULL_ID, f_id, seat));
    }
    open_seats_str += fmt::format(" are available on flight {}", f_id);
    Debug("%s", open_seats_str);
    Debug("COMMIT");

    return client.Commit(timeout);
}
}