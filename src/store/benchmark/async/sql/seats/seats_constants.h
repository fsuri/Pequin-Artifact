#ifndef SQL_SEATS_CONSTANTS_H 
#define SQL_SEATS_CONSTANTS_H

#include <ctime>
#include <string> 
#include <limits>
namespace seats_sql {

const int TOTAL_SEATS_PER_FLIGHT = 150;
const int MIN_SEATS_RESERVED = (int) (TOTAL_SEATS_PER_FLIGHT * 0.6);
const int MAX_SEATS_RESERVED = (int) (TOTAL_SEATS_PER_FLIGHT * 0.8);

const int NEW_RESERVATION_ATTRS_SIZE = 9;
const std::time_t MIN_TS = 1697218894000;
const std::time_t MAX_TS = MIN_TS + ((int64_t) 86400000) * 50;
const double MIN_RESERVATION_PRICE = 100;
const double MAX_RESERVATION_PRICE = 1000; //1500;
const int64_t NULL_ID = std::numeric_limits<int64_t>::min();
const int64_t MAX_BIG_INT = std::numeric_limits<int64_t>::max() - 1;
const double FLIGHT_TRAVEL_RATE = 570.0;

const int64_t MS_IN_DAY = 86400000L;

/** Frequency of Transaction in Benchmark Workload */
const int FREQUENCY_DELETE_RESERVATION = 10;
const int FREQUENCY_FIND_FLIGHTS = 10;
const int FREQUENCY_FIND_OPEN_SEATS = 35;
const int FREQUENCY_NEW_RESERVATION = 20;
const int FREQUENCY_UPDATE_CUSTOMER = 10;
const int FREQUENCY_UPDATE_RESERVATION = 15;


const int PROB_REQUEUE_DELETED_RESERVATION = 90;
const int PROB_DELETE_WITH_CUSTOMER_ID_STR = 20;
const int PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20;
const int PROB_UPDATE_FREQUENT_FLYER = 25;
const int PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 10;  //FIXME: Should be 25?  TODO: Add RANDOM_AIRPORTS 10?

/** Table Names */

const std::string COUNTRY_TABLE = "country";
const std::string AIRPORT_TABLE = "airport";
const std::string AIRPORT_DISTANCE_TABLE = "airport_distance";
const std::string AIRLINE_TABLE = "airline";
const std::string CUSTOMER_TABLE = "customer";
const std::string FREQUENT_FLYER_TABLE = "frequent_flyer";
const std::string FLIGHT_TABLE = "flight";
const std::string RESERVATION_TABLE = "reservation";

/** Table Size Information */

const int SCALE_FACTOR = 5;
const int NUM_CUSTOMERS = SCALE_FACTOR * 100000;
const int NUM_FLIGHTS = SCALE_FACTOR * 10000;

const int NUM_AIRPORTS = 9263;
const int NUM_COUNTRIES = 248;
const int NUM_AIRLINES = 1250;
}

#endif 