#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

#include <fmt/core.h>

namespace seats_sql {
SQLUpdateReservation::SQLUpdateReservation(uint32_t timeout, std::mt19937 &gen, SeatsProfile &profile)
    : SEATSSQLTransaction(timeout), gen_(&gen), profile(profile) {
        if (!profile.update_reservations.empty()) {
            SEATSReservation r = profile.update_reservations.front();
            r_id = r.r_id;
            c_id = r.c_id;
            flight = r.flight;
            f_id = flight.flight_id;
            curr_seat = r.seat_num;
            seatnum = std::uniform_int_distribution<int>(1, TOTAL_SEATS_PER_FLIGHT)(gen);
            while(seatnum == curr_seat) seatnum = std::uniform_int_distribution<int>(1, TOTAL_SEATS_PER_FLIGHT)(gen); //pick a new seat
            profile.update_reservations.pop();
        } else { 
            // no reservations to update so make this transaction fail
            Panic("should not be triggered");
        }
        attr_idx = std::uniform_int_distribution<int64_t>(0, 3)(gen);
        attr_val = std::uniform_int_distribution<int64_t>(1, 100000)(gen);
       

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
        Debug("Seat %ld is already reserved on flight %ld!", seatnum, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    if (results[1]->empty()) {
        Debug("Customer %ld does not have an existing reservation flight %ld", c_id, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
  
    //Note: We will read from cache
    query = fmt::format("UPDATE {} SET r_seat = {}, {} = {} WHERE r_id = {} AND r_c_id = {} AND r_f_id = {}", 
                        RESERVATION_TABLE, seatnum, reserve_seats[attr_idx], attr_val, r_id, c_id, f_id);
    client.Write(query, queryResult, timeout);
    if (!queryResult->has_rows_affected()) {
        Panic("Failed to update reservation");
        client.Abort(timeout);
        return ABORTED_USER;
    }

     Debug("COMMIT");
    auto result = client.Commit(timeout);
    if(result != transaction_status_t::COMMITTED) return result;

     //////////////// UPDATE PROFILE /////////////////////

    auto &seats = profile.getSeatsBitSet(f_id);
    seats[seatnum-1] = 1; //set new seat
    seats[curr_seat-1] = 0;      //unset old seat

    if (std::uniform_int_distribution<int>(1, 100)(*gen_) < PROB_Q_DELETE_RESERVATION){
        Debug("UPDATE_RES: PUSH TO DELETE Q. r_id: %d, c_id: %d, flight_id: %d", r_id, c_id, flight.flight_id);
        profile.delete_reservations.push(SEATSReservation(r_id, c_id, flight, seatnum));
    }
    else{
        Debug("UPDATE_RES: PUSH TO UPDATE Q. r_id: %d, c_id: %d, flight_id: %d", r_id, c_id, flight.flight_id);
        profile.update_reservations.push(SEATSReservation(r_id, c_id, flight, seatnum));
    }

    return result;
}
}

