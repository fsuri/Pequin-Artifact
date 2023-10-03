#ifndef SQL_SEATS_CONSTANTS_H 
#define SQL_SEATS_CONSTANTS_H

namespace seats_sql {

const int TOTAL_SEATS_PER_FLIGHT = 150;
const int NEW_RESERVATION_ATTRS_SIZE = 9;
const std::string COUNTRY_TABLE = "Country";
const std::string AIRPORT_TABLE = "Airport";
const std::string AIRPORT_DISTANCE_TABLE = "AirportDistance";
const std::string AIRLINE_TABLE = "Airline";
const std::string CUSTOMER_TABLE = "Customer";
const std::string FREQUENT_FLYER_TABLE = "FrequentFlyer";
const std::string FLIGHT_TABLE = "Flight";
const std::string RESERVATION_TABLE = "Reservation";
const int64_t NULL_ID = std::numeric_limits<int64_t>::min();
}

#endif