#ifndef SEATS_SQL_DELETE_RESERVATION_H
#define SEATS_SQL_DELETE_RESERVATION_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"

#include <random>
#include <queue>

#include "store/benchmark/async/sql/seats/seats_profile.h"

namespace seats_sql {

class SQLDeleteReservation: public SEATSSQLTransaction {
    public: 
        SQLDeleteReservation(uint32_t timeout, std::mt19937 &gen, SeatsProfile &profile);
        virtual ~SQLDeleteReservation();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        CachedFlight flight;
        int64_t f_id;
        int64_t c_id;
        std::string c_id_str;
        std::string ff_c_id_str;
        int64_t ff_al_id;
    
        std::mt19937 *gen_;
        SeatsProfile &profile;
};

// struct GetFlightsResultRow_2 {
// public: 
//     GetFlightsResultRow_2() : f_al_id(0), f_seats_left(0), f_iattr00(0), f_iattr01(0), f_iattr02(0), f_iattr03(0), f_iattr04(0), f_iattr05(0), f_iattr06(0), f_iattr07(0) {}
//     virtual ~GetFlightsResultRow_2() {} 
//     virtual void SetFields(int64_t f_al_id, int64_t f_seats_left, int64_t f_iattr00, int64_t f_iattr01, int64_t f_iattr02, int64_t f_iattr03, int64_t f_iattr04, int64_t f_iattr05, int64_t f_iattr06, int64_t f_iattr07) {
//         f_al_id = f_al_id;   //airline ID
//         f_seats_left = f_seats_left;
//         f_iattr00 = f_iattr00;
//         f_iattr01 = f_iattr01;
//         f_iattr02 = f_iattr02;
//         f_iattr03 = f_iattr03;
//         f_iattr04 = f_iattr04;
//         f_iattr05 = f_iattr05; 
//         f_iattr06 = f_iattr06; 
//         f_iattr07 = f_iattr07;
//     }
//     int64_t f_al_id;
//     int64_t f_seats_left;
//     int64_t f_iattr00;
//     int64_t f_iattr01;
//     int64_t f_iattr02;
//     int64_t f_iattr03;
//     int64_t f_iattr04;
//     int64_t f_iattr05;
//     int64_t f_iattr06;
//     int64_t f_iattr07;
// };

// void inline load_row(GetFlightsResultRow_2 &store, std::unique_ptr<query_result::Row> row) {
//     row->get(0, &store.f_al_id);
//     row->get(1, &store.f_seats_left);
//     row->get(2, &store.f_iattr00);
//     row->get(3, &store.f_iattr01);
//     row->get(4, &store.f_iattr02);
//     row->get(5, &store.f_iattr03); 
//     row->get(6, &store.f_iattr04);
//     row->get(7, &store.f_iattr05);
//     row->get(8, &store.f_iattr06);
//     row->get(9, &store.f_iattr07);
// }

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
    double r_price;
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

}
#endif

