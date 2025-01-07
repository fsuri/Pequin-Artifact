#ifndef SEATS_SQL_SCHEMA_H
#define SEATS_SQL_SCHEMA_H

#include <stdint.h>
#include <memory>
#include "store/benchmark/async/sql/seats/seats_transaction.h"

namespace seats_sql {

struct GetNearbyAirportsResultRow {
    GetNearbyAirportsResultRow() : dp_ap_id0(0), dp_ap_id1(0), d_distance(0) {} 
    ~GetNearbyAirportsResultRow() {} 
    int64_t dp_ap_id0;
    int64_t dp_ap_id1; 
    double d_distance;
};

void inline load_row(GetNearbyAirportsResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.dp_ap_id0);
    row->get(1, &store.dp_ap_id1);
    row->get(2, &store.d_distance);
}

struct GetFlightsResultRow {
public: 
    GetFlightsResultRow() : f_id(0), f_al_id(0), f_depart_ap_id(0), f_depart_time(0), f_arrive_ap_id(0), f_arrive_time(0), ap_name(""), al_iattr00(0), al_iattr01(0) {}
    virtual ~GetFlightsResultRow() {} 
    virtual void SetFields(int64_t f_id, int64_t f_al_id, int64_t f_depart_ap_id, int64_t f_depart_time, int64_t f_arrive_ap_id, int64_t f_arrive_time, std::string ap_name, int64_t al_iattr00, int64_t al_iattr01) {
        f_id = f_id; 
        f_al_id = f_al_id; 
        f_depart_ap_id = f_depart_ap_id;
        f_depart_time = f_depart_time; 
        f_arrive_ap_id = f_arrive_ap_id;
        f_arrive_time = f_arrive_time; 
        ap_name = ap_name;
        al_iattr00 = al_iattr00; 
        al_iattr01 = al_iattr01;
    }
    int64_t f_id; 
    int64_t f_al_id;
    int64_t f_depart_ap_id;
    int64_t f_depart_time;
    int64_t f_arrive_ap_id;
    int64_t f_arrive_time;
    std::string ap_name; 
    int64_t al_iattr00;
    int64_t al_iattr01;
};

void inline load_row(GetFlightsResultRow &store, std::unique_ptr<query_result::Row> row) {
    int64_t f_id; 
    int64_t f_al_id;
    int64_t f_depart_ap_id;
    int64_t f_depart_time;
    int64_t f_arrive_ap_id;
    int64_t f_arrive_time;
    std::string ap_name; 
    int64_t al_iattr00;
    int64_t al_iattr01;
    row->get(0, &f_id);
    row->get(1, &f_al_id);
    row->get(2, &f_depart_ap_id);
    row->get(3, &f_depart_time);
    row->get(4, &f_arrive_ap_id);
    row->get(5, &f_arrive_time); 
    row->get(6, &ap_name);
    row->get(7, &al_iattr00);
    row->get(8, &al_iattr01);
    store.SetFields(f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, ap_name, al_iattr00, al_iattr01);
}

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

struct GetFlightsResultRow_2 {
public: 
    GetFlightsResultRow_2() : f_al_id(0), f_seats_left(0), f_iattr00(0), f_iattr01(0), f_iattr02(0), f_iattr03(0), f_iattr04(0), f_iattr05(0), f_iattr06(0), f_iattr07(0) {}
    virtual ~GetFlightsResultRow_2() {} 
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

void inline load_row(GetFlightsResultRow_2 &store, std::unique_ptr<query_result::Row> row) {
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


//////////////



struct GetAirportInfoResultRow {
public:
    GetAirportInfoResultRow() : ap_code(""), ap_name(""), ap_city(""), ap_longitude(0.0), ap_latitude(0.0), co_id(0), co_name(""), co_code_2(""), co_code_3("") {}
    virtual ~GetAirportInfoResultRow() {}
    virtual void SetFields(std::string ap_code, std::string ap_name, std::string ap_city, float ap_longitude, float ap_latitude, int64_t co_id, std::string co_name, std::string co_code_2, std::string co_code_3) {
        ap_code = ap_code; 
        ap_name = ap_name; 
        ap_city = ap_city; 
        ap_longitude = ap_longitude; 
        ap_latitude = ap_latitude; 
        co_id = co_id; 
        co_name = co_name; 
        co_code_2 = co_code_2; 
        co_code_3 = co_code_3;
    }
    std::string ap_code;
    std::string ap_name; 
    std::string ap_city;
    double ap_longitude;
    double ap_latitude;
    int64_t co_id;
    std::string co_name;
    std::string co_code_2;
    std::string co_code_3;
};

void inline load_row(GetAirportInfoResultRow &store, std::unique_ptr<query_result::Row> row) {
    std::string ap_code;
    std::string ap_name; 
    std::string ap_city;
    double ap_longitude;
    double ap_latitude;
    int64_t co_id;
    std::string co_name;
    std::string co_code_2;
    std::string co_code_3;
    row->get(0, &ap_code);
    row->get(1, &ap_name);
    row->get(2, &ap_city);
    row->get(3, &ap_longitude);
    row->get(4, &ap_latitude);
    row->get(5, &co_id); 
    row->get(6, &co_name);
    row->get(7, &co_code_2);
    row->get(8, &co_code_3);
    store.SetFields(ap_code, ap_name, ap_city, ap_longitude, ap_latitude, co_id, co_name, co_code_2, co_code_3);
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



};
#endif