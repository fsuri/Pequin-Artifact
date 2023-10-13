#include "store/benchmark/async/sql/seats/new_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

#include <fmt/core.h>

namespace seats_sql {
    
SQLNewReservation::SQLNewReservation(uint32_t timeout, std::mt19937_64 gen, int64_t r_id) : 
    SEATSSQLTransaction(timeout), r_id(r_id), price(price) {
        c_id = std::uniform_int_distribution<int64_t>(1, NUM_CUSTOMERS)(gen);
        f_id = std::uniform_int_distribution<int64_t>(1, NUM_FLIGHTS)(gen);
        seatnum = std::uniform_int_distribution<int64_t>(1, TOTAL_SEATS_PER_FLIGHT)(gen);
        time = std::time(nullptr);
        attributes.reserve(NEW_RESERVATION_ATTRS_SIZE);
        auto attr_dist = std::uniform_int_distribution<int64_t>(1, 100000);
        for (int i = 0; i < NEW_RESERVATION_ATTRS_SIZE; i++) {
            attributes.push_back(attr_dist(gen));
        }
        price = std::uniform_real_distribution<double>(MIN_RESERVATION_PRICE, MAX_RESERVATION_PRICE)(gen);
    }

SQLNewReservation::~SQLNewReservation() {} 

transaction_status_t SQLNewReservation::Execute(SyncClient &client) {
    if (attributes.size() != NEW_RESERVATION_ATTRS_SIZE) 
        Panic("Wrong number of attributes (%d) given in NewReservation Transaction", attributes.size());

    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::unique_ptr<const query_result::QueryResult> queryResult2;
    std::unique_ptr<const query_result::QueryResult> queryResult3;

    std::string query;

    Debug("NEW_RESERVATION for customer %d", c_id);
    client.Begin(timeout);

    query = fmt::format("SELECT F_AL_ID, F_SEATS_LEFT, {}.* FROM {}, {} WHERE F_ID = {} AND F_AL_ID = AL_ID", AIRLINE_TABLE, FLIGHT_TABLE, AIRLINE_TABLE, f_id);
    client.Query(query, queryResult, timeout);
    query = fmt::format("SELECT R_ID FROM {} WHERE R_F_ID = {} AND R_SEAT = {}", RESERVATION_TABLE, f_id, seatnum);
    client.Query(query, queryResult2, timeout);
    query = fmt::format("SELECT R_ID FROM {} WHERE R_F_ID = {} AND R_C_ID = {}", RESERVATION_TABLE, f_id, c_id);
    client.Query(query, queryResult3, timeout);

    if (queryResult->empty()) {
        Debug("Invalid Flight ID %d", f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    } else if (!queryResult2->empty()) {
        Debug("Seat %d on flight %d is already reserved", seatnum, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    } else if (!queryResult3->empty()) {
        Debug("Customer %d already has a seat", c_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }

    int64_t airline_id;
    int64_t seats_left; 
    queryResult->at(0)->get(0, &airline_id);
    queryResult->at(0)->get(1, &seats_left);
    if (seats_left <= 0) {
        Debug("No more seats left on flight %d", f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }

    if (c_id != NULL_ID) {
        query = fmt::format("SELECT C_BASE_AP_ID, C_BALANCE, C_SATTR00, FROM {} WHERE C_ID = {}", CUSTOMER_TABLE, c_id);
        client.Query(query, queryResult, timeout);
        if (queryResult->empty()) {
            Debug("No Customer with id %d", c_id);
            client.Abort(timeout);
            return ABORTED_USER;
        }
    }

    bool updatedSuccessful = true;
    query = fmt::format("INSERT INTO {} (R_ID, R_C_ID, R_F_ID, R_SEAT, R_PRICE, R_IATTR00, R_IATTR01, R_IATTR02, R_IATTR03, R_IATTR04, R_IATTR05, R_IATTR06, R_IATTR07, R_IATTR08, R_CREATED, R_UPDATED) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}))",
                            RESERVATION_TABLE, r_id, c_id, f_id, seatnum, price, attributes[0], attributes[1], attributes[2], attributes[3], attributes[4], attributes[5], attributes[6], attributes[7], attributes[8], time, time);
    client.Write(query, queryResult, timeout);
    updatedSuccessful = updatedSuccessful && !queryResult->empty();
    query = fmt::format("UPDATE {} SET F_SEATS_LEFT = F_SEATS_LEFT - 1 WHERE F_ID = {}", FLIGHT_TABLE, f_id);
    client.Write(query, queryResult, timeout);
    updatedSuccessful = updatedSuccessful && !queryResult->empty();
    query = fmt::format("UPDATE {} SET C_IATTR10 = CIATTR10 + 1, C_IATTR11 = C_IATTR11 + 1, C_IATTR12 = {}, C_IATTR13 = {}, C_IATTR14 = {}, C_IATTR15 = {} WHERE C_ID = {}", 
                        CUSTOMER_TABLE, attributes[0], attributes[1], attributes[2], attributes[3], c_id);
    client.Write(query, queryResult, timeout);
    updatedSuccessful = updatedSuccessful && !queryResult->empty();
    query = fmt::format("UPDATE {} SET F_IATTR10 = F_IATTR10 + 1, F_IATTR11 = {}, F_IATTR12 = {}, FF_IATTR13 = {}, FF_IATTR14 = {} WHERE FF_C_ID = {} AND FF_AL_ID = {}", 
                        FREQUENT_FLYER_TABLE, attributes[4], attributes[5], attributes[6], attributes[7], c_id, airline_id);
    client.Write(query, queryResult, timeout);
    updatedSuccessful = updatedSuccessful && !queryResult->empty();

    if (!updatedSuccessful) {
        Debug("Updated failed for new reservation");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    return client.Commit(timeout);
}       
}