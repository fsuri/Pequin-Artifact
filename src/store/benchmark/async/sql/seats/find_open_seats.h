#ifndef SEATS_SQL_FIND_OPEN_SEATS_H
#define SEATS_SQL_FIND_OPEN_SEATS_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"
#include "store/benchmark/async/sql/seats/reservation.h"
#include <random>
#include <queue>

namespace seats_sql {

class SQLFindOpenSeats: public SEATSSQLTransaction {
    public: 
        SQLFindOpenSeats(uint32_t timeout, std::mt19937 &gen, std::queue<SEATSReservation> &new_res_queue, std::vector<CachedFlight> &cached_flight_ids);
        virtual ~SQLFindOpenSeats();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        CachedFlight f_id;  // flight id
        std::queue<SEATSReservation> *q;
        std::mt19937 *gen_;
};

struct GetFlightResultRow {
public: 
    GetFlightResultRow() : f_id(0), f_al_id(0), f_depart_ap_id(0), f_depart_time(0), f_arrive_ap_id(0), f_arrive_time(0), f_base_price(0.0), f_seats_total(0) {}
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
    //double f_price;
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
    //row->get(9, &store.f_price);
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

}

#endif

