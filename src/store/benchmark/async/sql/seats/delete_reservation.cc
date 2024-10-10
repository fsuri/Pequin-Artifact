#include "store/benchmark/async/sql/seats/delete_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>
#include <random> 

namespace seats_sql{

SQLDeleteReservation::SQLDeleteReservation(uint32_t timeout, std::mt19937 &gen, SeatsProfile &profile) : 
    SEATSSQLTransaction(timeout), gen_(&gen), profile(profile) {
        

        UW_ASSERT(!profile.delete_reservations.empty());
        SEATSReservation r = profile.delete_reservations.front();
        profile.delete_reservations.pop();

        std::cerr << "DELETE_RESERVATION: " << r.r_id  << std::endl;
        Debug("DELETE_RESERVATION");
        c_id = r.c_id; //Default: Delete using Customer Id
        flight = r.flight;
        f_id = flight.flight_id;
        c_id_str = "";
        ff_c_id_str = "";
        ff_al_id = NULL_ID;
       
        int rand = std::uniform_int_distribution<int>(1, 100)(gen);
        //Delete with the Customer's id as a string
        if (rand <= PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            c_id_str = std::to_string(c_id);
            c_id = NULL_ID;
        }
        //Delete using FrequentFlyer information
        else if(rand <= PROB_DELETE_WITH_CUSTOMER_ID_STR + PROB_DELETE_WITH_FREQUENTFLYER_ID_STR){
            ff_c_id_str = std::to_string(c_id);
            c_id = NULL_ID;
            ff_al_id = flight.airline_id; 
        }
        //Default: Delete using their CustomerId 
    }

SQLDeleteReservation::~SQLDeleteReservation() {}


transaction_status_t SQLDeleteReservation::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    client.Begin(timeout);

    // //Get Flight attributes from Flight Id
    // query = fmt::format("SELECT f_al_id, f_seats_left, f_iattr00, f_iattr01, f_iattr02, f_iattr03, f_iattr04, f_iattr05, f_iattr06, f_iattr07 FROM {} WHERE f_id = {}", FLIGHT_TABLE, f_id);
    // client.Query(query, queryResult, timeout);
    // if (queryResult->empty()) {
    //     Debug("Flight id %ld not found in Flights Table", f_id);
    //     client.Abort(timeout);
    //     return ABORTED_USER;
    // }
    // GetFlightsResultRow_2 fr_row = GetFlightsResultRow_2();
    // deserialize(fr_row, queryResult, 0);
    // int64_t ff_al_id = fr_row.f_al_id;
    

    //If we weren't given the customer id, then look it up
    if (c_id == NULL_ID) {
        bool has_al_id = false;
        if (c_id_str.size() > 0) {
            //(A) Get Customer ID by Customer IdStr
            query = fmt::format("SELECT c_id FROM {} WHERE c_id_str = '{}'", CUSTOMER_TABLE, c_id_str);
        } else if (ff_c_id_str.size() > 0){ // && ff_al_id != NULL_ID) {
            //(B) Get Customer ID by Frequent Flyer Number
            has_al_id = true;
            query = fmt::format("SELECT c_id, ff_al_id FROM {}, {} WHERE ff_c_id_str = '{}' AND ff_c_id = c_id "
            "AND c_id = c_id", //REFLEXIVE ARGS TO SATISFY DUMB PELOTON PLANNER
                                FREQUENT_FLYER_TABLE, CUSTOMER_TABLE, ff_c_id_str, ff_c_id_str);
        } else {
            Debug("No way to get Customer ID");
            client.Abort(timeout);
            return ABORTED_USER;
        }
        client.Query(query, queryResult, timeout);
        if (queryResult->empty()) {
            Debug("No customer record found");
            client.Abort(timeout);
            return ABORTED_USER;
        }
        deserialize(c_id, queryResult, 0, 0);
        if(has_al_id) deserialize(ff_al_id, queryResult, 0, 1);  //Pick the first airline//FIXME: This does not make much sense... -> Should pick Al_ID that belongs to the reservation.
    }

    //Next, get the reservation information of the Customer (GetCustomerReservation)
    // query = fmt::format("SELECT c_sattr00, c_sattr02, c_sattr04, c_iattr00, c_iattr02, c_iattr04, c_iattr06, f_seats_left, r_id, r_seat, r_price, r_iattr00 FROM {}, {}, {} "
    //                     "WHERE c_id = {} AND c_id = r_c_id AND f_id = {} AND f_id = r_f_id", CUSTOMER_TABLE, FLIGHT_TABLE, RESERVATION_TABLE, c_id, f_id);
   
    query = fmt::format("SELECT c_sattr00, c_sattr02, c_sattr04, c_iattr00, c_iattr02, c_iattr04, c_iattr06, f_seats_left, r_id, r_seat, r_price, r_iattr00 FROM {}, {}, {} "
                        "WHERE c_id = {} AND c_id = r_c_id "
                        "AND f_id = {} AND f_id = r_f_id "
                        "AND r_f_id = r_f_id", //REFLEXIVE ARGS TO SATISFY DUMB PELOTON PLANNER
                        CUSTOMER_TABLE, FLIGHT_TABLE, RESERVATION_TABLE, c_id, f_id);
                   
    client.Query(query, queryResult, timeout);
    //If there is no valid customer record, throw an abort  //Note: supposedly happens 5% of the time.
    if (queryResult->empty()) {
        Debug("No customer record with id %ld", c_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    GetCustomerReservationRow cr_row;
    deserialize(cr_row, queryResult, 0);
    int64_t c_iattr00 = cr_row.c_iattr00;
    int64_t seats_left = cr_row.f_seats_left;
    int64_t r_id = cr_row.r_id;
    double r_price = cr_row.r_price;
    int64_t seat = cr_row.r_seat;

    //Allow all writes below to be in parallel.
    std::vector<std::unique_ptr<const query_result::QueryResult>> results;

    // Now Delete Reservation
    query = fmt::format("DELETE FROM {} WHERE r_id = {} AND r_c_id = {} AND r_f_id = {}", RESERVATION_TABLE, r_id, c_id, f_id);
    client.Write(query, timeout);
    //if(!queryResult->has_rows_affected()){ Panic("Failed to update frequent flyer info");}

    // Update Flight
    query = fmt::format("UPDATE {} SET f_seats_left = f_seats_left + 1 WHERE f_id = {}", FLIGHT_TABLE, f_id);
    client.Write(query, timeout);
  
    //Update Customer
    query = fmt::format("UPDATE {} SET c_balance = c_balance + {}, c_iattr00 = {}, c_iattr10 = c_iattr10 - 1, c_iattr11 = c_iattr11 - 1 WHERE c_id = {}", CUSTOMER_TABLE, -1 * r_price, c_iattr00, c_id);
    client.Write(query, timeout);

    bool update_freq_flyer = ff_al_id != NULL_ID;
    //Update FrequentFlyer
    if (update_freq_flyer) {
        query = fmt::format("UPDATE {} SET ff_iattr10 = ff_iattr10 - 1 WHERE ff_c_id = {} AND ff_al_id = {}", FREQUENT_FLYER_TABLE, c_id, ff_al_id);
        client.Write(query, timeout);
    }

    client.Wait(results);
    //Debug
    Debug("results size: %d", results.size());
    UW_ASSERT(results.size() == 4 - !update_freq_flyer);
    bool abort = false;
    if(!results[0]->has_rows_affected()){ Panic("Failed to delete reservation"); abort = true;}
    if(!results[1]->has_rows_affected()){ Panic("Failed to update number of seats left in flight"); abort = true;}
    if(!results[2]->has_rows_affected()){ Panic("Failed to update customer balance"); abort = true;}
    if(update_freq_flyer && !results[3]->has_rows_affected()){ Panic("Failed to update frequent flyer info");} //We don't care if we updated FrequentFlyer
    if(abort){
        client.Abort(timeout);
        return ABORTED_USER;
    }

    auto result = client.Commit(timeout);
    if(result != transaction_status_t::COMMITTED) return result;


     //////////////// UPDATE PROFILE /////////////////////
    
    //Remove booking from cache
    auto &seats = profile.getSeatsBitSet(f_id);
    seats[seat-1] = 0;
    //profile.deleteCustomerBooking(c_id, f_id);

    //Re-queue reservation
    int requeue = std::uniform_int_distribution<int>(1, 100)(*gen_);
    if (requeue <= PROB_REQUEUE_DELETED_RESERVATION) profile.insert_reservations.push(SEATSReservation(NULL_ID, c_id, flight, seat));

    Debug("Deleted reservation on flight %s for customer %d. [seatsLeft=%d]", f_id, c_id, seats_left + 1);
    

    return result;
}
}