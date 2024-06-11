#include "store/benchmark/async/sql/seats/find_open_seats.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

#include <fmt/core.h>
#include <queue>


namespace seats_sql {

SQLFindOpenSeats::SQLFindOpenSeats(uint32_t timeout, std::mt19937 &gen, SeatsProfile &profile) : 
    SEATSSQLTransaction(timeout), gen_(&gen), profile(profile) {

        int64_t flight_index = std::uniform_int_distribution<int64_t>(1, profile.cached_flights.size())(gen) - 1; //TODO: Turn into a profile function.
        CachedFlight &flight = profile.cached_flights[flight_index];

        f_id = flight;
    }

SQLFindOpenSeats::~SQLFindOpenSeats() {};

transaction_status_t SQLFindOpenSeats::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    fprintf(stderr, "FIND_OPEN_SEATS on flight %ld \n", f_id.flight_id);
    Debug("FIND_OPEN_SEATS on flight %ld", f_id.flight_id);
    client.Begin(timeout);

    GetFlightResultRow fr_row = GetFlightResultRow();
    //GetFlight   //Doing computation in the Select statement is tricky for the point read validation to check. Thus we removed it from the query statement here and do the calculation ourselves.
    // query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, f_base_price, f_seats_total, f_seats_left, "
    //                     "(f_base_price + (f_base_price * (1 - (f_seats_left / f_seats_total)))) AS f_price FROM {} WHERE f_id = {}", FLIGHT_TABLE, f_id);
    query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, f_base_price, f_seats_total, f_seats_left "
                        "FROM {} WHERE f_id = {}", FLIGHT_TABLE, f_id.flight_id);
    client.Query(query, queryResult, timeout);
    if (queryResult->empty()) { 
        Notice("No flight with id % exists.", f_id.flight_id);
        Debug("no flight with that id exists");
        client.Abort(timeout);
        return ABORTED_USER;
    }
    deserialize(fr_row, queryResult, 0);
    double base_price = fr_row.f_base_price;
    int64_t seats_total = fr_row.f_seats_total;
    int64_t seats_left = fr_row.f_seats_left;
    //double seat_price = fr_row.f_price;
    double _seat_price = base_price + (base_price * (1.0 - (seats_left/((double)seats_total))));

    query = fmt::format("SELECT r_id, r_c_id, r_f_id, r_seat FROM {} WHERE r_f_id = {}", RESERVATION_TABLE, f_id.flight_id);
    client.Query(query, queryResult, timeout);

    Debug("COMMIT");
    auto result = client.Commit(timeout);
    if(result != transaction_status_t::COMMITTED) return result;


    //////////////// UPDATE PROFILE /////////////////////

    // int8_t unavailable_seats[TOTAL_SEATS_PER_FLIGHT];
    auto &unavailable_seats = profile.getSeatsBitSet(f_id.flight_id);
    //memset(unavailable_seats, 0, TOTAL_SEATS_PER_FLIGHT);
    profile.resetFlightCache(f_id.flight_id);

    GetSeatsResultRow sr_row;

    for (std::size_t i = 0; i < queryResult->size(); i++) {
        deserialize(sr_row, queryResult, i);
        int seat = (int) sr_row.r_seat - 1; //seats are numbered from 1 (not from 0)
        unavailable_seats[seat] = (int8_t) 1;
        //std::cerr << "SEAT: " << i << "is unavailable!" << std::endl;
        //profile.cacheCustomerBooking(sr_row.r_c_id, f_id.flight_id);
    }

    auto airport_depart_id = f_id.depart_ap_id;

    std::string open_seats_str = "Seats [";
    std::string reserved_seats_str = "Seats [";
  
    std::deque<SEATSReservation> tmp;
    for (int seat = 1; seat <= TOTAL_SEATS_PER_FLIGHT; seat++) {   
        if (unavailable_seats[seat-1] == 1) reserved_seats_str += fmt::format(" {},", seat);

        if (unavailable_seats[seat-1] == 0) {
            open_seats_str += fmt::format(" {},", seat);
            //TODO:? Benchbase also returns a price (that is higher for first class seats) -- but it seems to be unused.

            int64_t c_id = profile.getRandomCustomerId(airport_depart_id);
            //Pick random customer if customer is already booked on this flight.
            while(profile.isCustomerBookedOnFlight(c_id, f_id.flight_id)){
                c_id = profile.getRandomCustomerId();
            }
        
            //std::cerr << "FIND OPEN SEAT: PUSH TO INSERT Q. c_id: " << c_id << ". f_id: " << f_id.flight_id << ". seat: " << seat << std::endl;
            tmp.push_back(SEATSReservation(NULL_ID, c_id, f_id, seat));   //TODO: set the f_al_id too? FIXME: FlightID != single id.  //TODO: id should be the clients id.? No r_id?
        }
    }

    //shuffle randomly and then push back a subset of the open seats for new reservations
    std::random_shuffle(tmp.begin(), tmp.end());

    while(!tmp.empty()){
        profile.insert_reservations.push(tmp.front());
        tmp.pop_front();
    }
    while(profile.insert_reservations.size() > MAX_PENDING_INSERTS){
        profile.insert_reservations.pop();
    }

    open_seats_str += fmt::format("] are available on flight {} for price {}", f_id.flight_id, _seat_price);
    reserved_seats_str += fmt::format("] are unavailable on flight {}", f_id.flight_id);
    Debug("%s", open_seats_str);
    fprintf(stderr, "Available: %s\n", open_seats_str.c_str());
    fprintf(stderr, "Unavailable: %s\n", reserved_seats_str.c_str());

    return result;
}
}