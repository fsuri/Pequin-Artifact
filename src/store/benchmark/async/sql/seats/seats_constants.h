#ifndef SQL_SEATS_CONSTANTS_H 
#define SQL_SEATS_CONSTANTS_H

namespace seats_sql {

const int TOTAL_SEATS_PER_FLIGHT = 150;
const int MIN_SEATS_RESERVED = (int) (TOTAL_SEATS_PER_FLIGHT * 0.6);
const int MAX_SEATS_RESERVED = (int) (TOTAL_SEATS_PER_FLIGHT * 0.8);

const int NEW_RESERVATION_ATTRS_SIZE = 9;
const std::time_t MIN_TS = 1697218894000;
const std::time_t MAX_TS = MIN_TS + 86400000 * 50;
const double MIN_RESERVATION_PRICE = 100;
const double MAX_RESERVATION_PRICE = 1500;
const int64_t NULL_ID = std::numeric_limits<int64_t>::min();

/** Frequency of Transaction in Benchmark Workload */

const int FREQUENCY_DELETE_RESERVATION = 10;
const int FREQUENCY_FIND_FLIGHTS = 10;
const int FREQUENCY_FIND_OPEN_SEATS = 35;
const int FREQUENCY_NEW_RESERVATION = 20;
const int FREQUENCY_UPDATE_CUSTOMER = 10;
const int FREQUENCY_UPDATE_RESERVATION = 15;

/** Table Names */

const std::string COUNTRY_TABLE = "Country";
const std::string AIRPORT_TABLE = "Airport";
const std::string AIRPORT_DISTANCE_TABLE = "AirportDistance";
const std::string AIRLINE_TABLE = "Airline";
const std::string CUSTOMER_TABLE = "Customer";
const std::string FREQUENT_FLYER_TABLE = "FrequentFlyer";
const std::string FLIGHT_TABLE = "Flight";
const std::string RESERVATION_TABLE = "Reservation";

/** Table Size Information */

const int SCALE_FACTOR = 5;
const int NUM_CUSTOMERS = SCALE_FACTOR * 100000;
const int NUM_FLIGHTS = SCALE_FACTOR * 10000;

const int NUM_AIRPORTS = 9263;
const int NUM_COUNTRIES = 248;
const int NUM_AIRLINES = 1250;
}

#endif 