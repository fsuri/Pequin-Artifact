#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include "store/benchmark/async/sql/seats/reservation.h"
#include <fmt/core.h>

namespace seats_sql {
SQLUpdateReservation::SQLUpdateReservation(uint32_t timeout, std::mt19937 &gen, std::queue<SEATSReservation> &update_res, std::queue<SEATSReservation> &delete_res)
    : SEATSSQLTransaction(timeout), gen_(&gen) {
        if (!update_res.empty()) {
            SEATSReservation r = update_res.front();
            r_id = r.r_id;
            c_id = r.c_id;
            flight = r.flight;
            f_id = flight.flight_id;
            seatnum = std::uniform_int_distribution<int>(1, TOTAL_SEATS_PER_FLIGHT)(gen);
            while(seatnum == r.seat_num) seatnum = std::uniform_int_distribution<int>(1, TOTAL_SEATS_PER_FLIGHT)(gen); //pick a new seat
            update_res.pop();
        } else { 
            // no reservations to update so make this transaction fail
            Panic("should not be triggered");
            c_id = NULL_ID;
            r_id = NULL_ID;
            flight = CachedFlight();
            f_id = NULL_ID;
            seatnum = 0;
        }
        attr_idx = std::uniform_int_distribution<int64_t>(0, 3)(gen);
        attr_val = std::uniform_int_distribution<int64_t>(1, 100000)(gen);
        update_q = &update_res;
        delete_q = &delete_res;

        std::cerr << "UPDATE_RESERVATION: " << r_id << ". Flight:" << f_id << ". New seat: " << seatnum << std::endl;
        Debug("UPDATE_RESERVATION");

    }

SQLUpdateReservation::~SQLUpdateReservation() {}

transaction_status_t SQLUpdateReservation::Execute(SyncClient &client) {

    if(c_id == NULL_ID) return ABORTED_USER;

    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::unique_ptr<const query_result::QueryResult> queryResult2;

    std::vector<std::unique_ptr<const query_result::QueryResult>> results; 
    std::string query;

    client.Begin(timeout);

    // (1) Check if Seat is taken (CheckSeat)
    query = fmt::format("SELECT r_id FROM {} WHERE r_f_id = {} AND r_seat = {}", RESERVATION_TABLE, f_id, seatnum);
    client.Query(query, timeout);

    // (2) Check that Customer already has a Seat (CheckCustomer)
    //query = fmt::format("SELECT r_id FROM {} WHERE r_f_id = {} AND r_c_id = {}", RESERVATION_TABLE, f_id, c_id);
    query = fmt::format("SELECT * FROM {} WHERE r_id = {} AND r_c_id = {} AND r_f_id = {}", RESERVATION_TABLE, r_id, c_id, f_id); //Do point lookup
    client.Query(query, timeout);

    client.Wait(results); //execute the two reads in parallel

    if (!results[0]->empty()) {
        Notice("Seat %ld is already reserved on flight %ld!", seatnum, f_id);
        Debug("Seat %ld is already reserved on flight %ld!", seatnum, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    if (results[1]->empty()) {
        Notice("Customer %ld does not have an existing reservation flight %ld", c_id, f_id);
        Debug("Customer %ld does not have an existing reservation flight %ld", c_id, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
  
    query = fmt::format("UPDATE {} SET r_seat = {}, {} = {} WHERE r_id = {} AND r_c_id = {} AND r_f_id = {}", 
                        RESERVATION_TABLE, seatnum, reserve_seats[attr_idx], attr_val, r_id, c_id, f_id);
    client.Write(query, queryResult, timeout);
    if (!queryResult->has_rows_affected()) {
        Panic("Failed to update reservation");
        client.Abort(timeout);
        return ABORTED_USER;
    }

    if (std::uniform_int_distribution<int>(1, 100)(*gen_) < PROB_Q_DELETE_RESERVATION){
        std::cerr << "Update_RES: PUSH TO DELETE Q" << std::endl;
        delete_q->push(SEATSReservation(r_id, c_id, flight, seatnum));
    }
    else{
         std::cerr << "Update_RES: PUSH TO UPDATE Q" << std::endl;
        update_q->push(SEATSReservation(r_id, c_id, flight, seatnum));
    }

    return client.Commit(timeout);
}
}

