#include "store/benchmark/async/sql/seats/new_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

#include <fmt/core.h>
#include <queue>

namespace seats_sql {
    
SQLNewReservation::SQLNewReservation(uint32_t timeout, std::mt19937 &gen, int64_t r_id, std::queue<SEATSReservation> &insert_res, std::queue<SEATSReservation> &update_res, std::queue<SEATSReservation> &delete_res) : 
    SEATSSQLTransaction(timeout), r_id(r_id), gen_(&gen) {
        if (!insert_res.empty()) {
            SEATSReservation res = insert_res.front();
            f_id = res.f_id; 
            seatnum = res.seat_num;
            if (res.c_id != NULL_ID) 
                c_id = res.c_id;
            else
                c_id = std::uniform_int_distribution<int64_t>(1, NUM_CUSTOMERS)(gen);
            
            if (seatnum == -1)   
                seatnum = std::uniform_int_distribution<int64_t>(1, TOTAL_SEATS_PER_FLIGHT)(gen);

            insert_res.pop();
        } else { 
            Panic("should not be triggered");
            f_id = std::uniform_int_distribution<int64_t>(1, NUM_FLIGHTS)(gen);
            seatnum = std::uniform_int_distribution<int64_t>(1, TOTAL_SEATS_PER_FLIGHT)(gen);
            c_id = std::uniform_int_distribution<int64_t>(1, NUM_CUSTOMERS)(gen);
        }
        time = std::time(nullptr);
        attributes.reserve(NEW_RESERVATION_ATTRS_SIZE);
        auto attr_dist = std::uniform_int_distribution<int64_t>(1, 100000);
        for (int i = 0; i < NEW_RESERVATION_ATTRS_SIZE; i++) {
            attributes.push_back(attr_dist(gen));
        }
        price = std::uniform_real_distribution<double>(MIN_RESERVATION_PRICE, MAX_RESERVATION_PRICE)(gen); //TODO: Should be 2x this?
        update_q = &update_res;
        delete_q = &delete_res;
    }

SQLNewReservation::~SQLNewReservation() {} 

transaction_status_t SQLNewReservation::Execute(SyncClient &client) {
    if (attributes.size() != NEW_RESERVATION_ATTRS_SIZE) 
        Panic("Wrong number of attributes (%ld) given in NewReservation Transaction", attributes.size());

    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::unique_ptr<const query_result::QueryResult> queryResult2;
    std::unique_ptr<const query_result::QueryResult> queryResult3;

    std::vector<std::unique_ptr<const query_result::QueryResult>> results; 

    std::string query;

    fprintf(stderr,"NEW_RESERVATION for customer %ld. Flight: %d. Seatnum: %d.  \n", c_id, f_id, seatnum);
    Debug("NEW_RESERVATION for customer %ld", c_id);
    client.Begin(timeout);

    // (1) Get Flight information. (GetFlight)
    query = fmt::format("SELECT f_al_id, f_seats_left, al_iata_code, al_icao_code, al_call_sign, al_name, al_co_id FROM {}, {} WHERE f_id = {} AND f_al_id = al_id", FLIGHT_TABLE, AIRLINE_TABLE, f_id); 
    //Peloton does not support `.*` semantics. Replaced by just getting a couple (not all) airline fields.
    //query = fmt::format("SELECT f_al_id, f_seats_left, {}.* FROM {}, {} WHERE f_id = {} AND f_al_id = al_id", AIRLINE_TABLE, FLIGHT_TABLE, AIRLINE_TABLE, f_id); 
    client.Query(query, timeout); 

    // (2) Check whether Seat is available  (CheckSeat)
    query = fmt::format("SELECT r_id FROM {} WHERE r_f_id = {} AND r_seat = {}", RESERVATION_TABLE, f_id, seatnum);
    client.Query(query, timeout);
    // (3) Check whether Customer already has a seat  (CheckCustomer)
    query = fmt::format("SELECT r_id FROM {} WHERE r_f_id = {} AND r_c_id = {}", RESERVATION_TABLE, f_id, c_id);
      //todo? replace with single query? query = fmt::format("SELECT r_id FROM {} WHERE r_f_id = {} AND (r_c_id = {} OR r_seat = {}) ", RESERVATION_TABLE, f_id, c_id, seatnum);
    client.Query(query, timeout);

    // GetCustomer
    //query = fmt::format("SELECT c_base_ap_id, c_balance, c_sattr00 FROM {} WHERE c_id = {}", CUSTOMER_TABLE, c_id);
    query = fmt::format("SELECT * FROM {} WHERE c_id = {}", CUSTOMER_TABLE, c_id); //Use Select * to allow the point UPDATE to Customer to be processed from cache
    client.Query(query, timeout);
   
    client.Wait(results); //Execute the 4 reads in parallel
    
    int64_t airline_id;
    int64_t seats_left; 
    //If flight info not found => Abort
    if (results[0]->empty()) {
        Notice("Invalid Flight ID %ld", f_id);
        Debug("Invalid Flight ID %ld", f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    } 
    else{
        results[0]->at(0)->get(0, &airline_id);
        results[0]->at(0)->get(1, &seats_left);
        if (seats_left <= 0) {
            Notice("No more seats left on flight %ld", f_id);
            Debug("No more seats left on flight %ld", f_id);
            client.Abort(timeout);
            return ABORTED_USER;
    }
    }
    //If requested seat is not available => abort
    if (!results[1]->empty()) {
        Notice("Seat %ld on flight %ld is already reserved", seatnum, f_id);
        Debug("Seat %ld on flight %ld is already reserved", seatnum, f_id);
        client.Abort(timeout);
        return ABORTED_USER;
    } 
    //If customer already has a seat => abort
    if (!results[2]->empty()) {
        Notice("Customer %ld already has a seat", c_id);
        Debug("Customer %ld already has a seat", c_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    if (results[3]->empty()) {
        Notice("No Customer with id %ld", c_id);
        Debug("No Customer with id %ld", c_id);
        client.Abort(timeout);
        return ABORTED_USER;
    }
    
    bool updatedSuccessful = true;
    //InsertReservation
    query = fmt::format("INSERT INTO {} (r_id, r_c_id, r_f_id, r_seat, r_price, r_iattr00, r_iattr01, r_iattr02, r_iattr03, r_iattr04, r_iattr05, r_iattr06, r_iattr07, r_iattr08) "
                        "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})", RESERVATION_TABLE, r_id, c_id, f_id, seatnum, price, 
                        attributes[0], attributes[1], attributes[2], attributes[3], attributes[4], attributes[5], attributes[6], attributes[7], attributes[8]); 
    client.Write(query, timeout);
    //updatedSuccessful = (updatedSuccessful && queryResult->has_rows_affected());

    //Update Flight
    query = fmt::format("UPDATE {} SET f_seats_left = f_seats_left - 1 WHERE f_id = {}", FLIGHT_TABLE, f_id);
    client.Write(query, timeout);
    //updatedSuccessful = updatedSuccessful && queryResult->has_rows_affected();

    //UpdateCustomer
    query = fmt::format("UPDATE {} SET c_iattr10 = c_iattr10 + 1, c_iattr11 = c_iattr11 + 1, c_iattr12 = {}, c_iattr13 = {}, c_iattr14 = {}, c_iattr15 = {} WHERE c_id = {}", 
                        CUSTOMER_TABLE, attributes[0], attributes[1], attributes[2], attributes[3], c_id);
    client.Write(query, timeout);
    //updatedSuccessful = updatedSuccessful && queryResult->has_rows_affected();

    //UpdateFrequentFlyer
    query = fmt::format("UPDATE {} SET ff_iattr10 = ff_iattr10 + 1, ff_iattr11 = {}, ff_iattr12 = {}, ff_iattr13 = {}, ff_iattr14 = {} WHERE ff_c_id = {} AND ff_al_id = {}", 
                        FREQUENT_FLYER_TABLE, attributes[4], attributes[5], attributes[6], attributes[7], c_id, airline_id);
    client.Write(query, timeout);
    //updatedSuccessful = updatedSuccessful && queryResult->has_rows_affected();

    client.Wait(results);

     UW_ASSERT(results.size() == 4);
    bool abort = false;
    if(!results[0]->has_rows_affected()){ Panic("Failed to insert Reservation"); abort = true;}
    if(!results[1]->has_rows_affected()){ Panic("Failed to update number of seats left in flight"); abort = true;}
    if(!results[2]->has_rows_affected()){ Panic("Failed to update customer attributes"); abort = true;}
    if(!results[3]->has_rows_affected()){ Debug("Failed to update frequent flyer info.");} //We don't care if we updated FrequentFlyer
    if(abort){
        client.Abort(timeout);
        return ABORTED_USER;
    }


    if (std::uniform_int_distribution<int>(1, 100)(*gen_) < PROB_Q_DELETE_RESERVATION){
        std::cerr << "NEW_RES: PUSH TO DELETE Q" << std::endl;
        delete_q->push(SEATSReservation(r_id, c_id, f_id, seatnum));
    }
    else{
         std::cerr << "NEW_RES: PUSH TO UPDATE Q" << std::endl;
        update_q->push(SEATSReservation(r_id, c_id, f_id, seatnum));
    }

    return client.Commit(timeout);
}       
}