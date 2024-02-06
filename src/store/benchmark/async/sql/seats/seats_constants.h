#ifndef SQL_SEATS_CONSTANTS_H 
#define SQL_SEATS_CONSTANTS_H

#include <ctime>
#include <string> 
#include <limits>
#include <vector>
namespace seats_sql {

const int SCALE_FACTOR = 1; //5;

const int64_t NULL_ID = std::numeric_limits<int64_t>::min();
const int64_t MAX_BIG_INT = std::numeric_limits<int64_t>::max() - 1;

// ----------------------------------------------------------------
// EXECUTION FREQUENCIES (0% - 100%)
// --------
/** Frequency of Transaction in Benchmark Workload */
const int FREQUENCY_DELETE_RESERVATION = 10;
const int FREQUENCY_FIND_FLIGHTS = 10;
const int FREQUENCY_FIND_OPEN_SEATS = 35;
const int FREQUENCY_NEW_RESERVATION = 20;
const int FREQUENCY_UPDATE_CUSTOMER = 10;
const int FREQUENCY_UPDATE_RESERVATION = 15;

// ----------------------------------------------------------------
// FLIGHT CONSTANTS
// ----------------------------------------------------------------

//The different distances that we can look-up for nearby airports. This is similar to the customer selecting a dropdown when looking for flights
const std::vector<int> NEAR_DISTANCES = {5, 10, 25, 50, 100};
const int MAX_NEAR_DISTANCE = NEAR_DISTANCES.back();

const int NUM_FLIGHTS = SCALE_FACTOR * 10000; //FIXME: WHERE IS THIS FROM?

/** The number of days in the past and future that we will generate flight information for */
//TODO: FLIGHTS_DAYS_PAST = 1;  //FIXME: Used by loader to generate flights
//TODO: FLIGHTs_DAYS_FUTURES = 50;
//TODO: FLIGHTS_PER_DAY_MIN = 1125; //FIXME: Used by loader to generate reservations
//TODO: FLIGHTS_PER_DAY_MAX = 1875;

//Number of seats available per flight 
const int TOTAL_SEATS_PER_FLIGHT = 150;

/** How many First Class seats are on a given flight These reservations are more expensive */
//TODO: FLIGHTS_FIRST_CLASS_OFFSET = 10

/** The rate in which a flight can travel between two airports (miles per hour) */
const double FLIGHT_TRAVEL_RATE = 570.0;

// ----------------------------------------------------------------
// CUSTOMER CONSTANTS
// ----------------------------------------------------------------

/** Default number of customers in the database */
const int NUM_CUSTOMERS = SCALE_FACTOR * 100000;

/** Max Number of FREQUENT_FLYER records per CUSTOMER */
 //TODO: int CUSTOMER_NUM_FREQUENTFLYERS_MIN = 0; //FIXME: USED in Loader to generate Frequentflyer Table.
 //TODO: int CUSTOMER_NUM_FREQUENTFLYERS_MAX = 10;
 //TODO:  double CUSTOMER_NUM_FREQUENTFLYERS_SIGMA = 2.0;

// The maximum number of days that we allow a customer to wait before needing a reservation on a return to their original departure airport
  //TODO: int CUSTOMER_RETURN_FLIGHT_DAYS_MIN = 1;  //FIXME: Used in Loader Reservation class?
  //TODO: int CUSTOMER_RETURN_FLIGHT_DAYS_MAX = 14;

// ----------------------------------------------------------------
// RESERVATION CONSTANTS
// ----------------------------------------------------------------

const int MIN_SEATS_RESERVED = (int) (TOTAL_SEATS_PER_FLIGHT * 0.6); //90    //FIXME: Where do these come from?
const int MAX_SEATS_RESERVED = (int) (TOTAL_SEATS_PER_FLIGHT * 0.8); //120

const int NEW_RESERVATION_ATTRS_SIZE = 9;

const double MIN_RESERVATION_PRICE = 100;
const double MAX_RESERVATION_PRICE = 1000; 

//int MAX_OPEN_SEATS_PER_TXN = 100; //UNUSED in original.

// ----------------------------------------------------------------
// PROBABILITIES
// ----------------------------------------------------------------

 /** Probability that a customer books a non-roundtrip flight (0% - 100%) */
//TODO: PROB_SINGLE_FLIGHT_RESERVATION = 10 //FIXME: Used in loader

//Probability that a customer will invoke DeleteReservation using the string version of their Customer Id (0% - 100%)
const int PROB_DELETE_WITH_CUSTOMER_ID_STR = 20;

//Probability that a customer will invoke UpdateCustomer using the string version of their Customer Id (0% - 100%)
const int PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20;

//Probability that a customer will invoke DeleteReservation using the string version of their FrequentFlyer Id (0% - 100%)
const int PROB_DELETE_WITH_FREQUENTFLYER_ID_STR = 20; //FIXME: Used for DeleteReservation

/** Probability that is a seat is initially occupied (0% - 100%) */
const int PROB_SEAT_OCCUPIED = 1; //25 

/** Probability that UpdateCustomer should update FrequentFlyer records */
const int PROB_UPDATE_FREQUENT_FLYER = 25;

 /** Probability that a new Reservation will be added to the DeleteReservation queue */
const int PROB_Q_DELETE_RESERVATION = 50;
 /** Probability that a new Reservation will be added to the UpdateReservation queue */
const int PROB_Q_UPDATE_RESERVATION = 100 - PROB_Q_DELETE_RESERVATION;
/** Probability that a deleted Reservation will be requeued for another NewReservation call */
const int PROB_REQUEUE_DELETED_RESERVATION = 90;

 /** Probability that FindFlights will use the distance search */
const int PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 25;  

/** Probability that FindFlights will use two random airports as its input */
//TODO: const int PROB_FIND_FLIGHTS_RANDOM_AIRPORTS = 10; //FIXME: Used for FindFlights

// ----------------------------------------------------------------
// TIME CONSTANTS
// ----------------------------------------------------------------

/** Number of microseconds in a day */
const int64_t MS_IN_DAY = 86400000L;

// Oldest time.
const std::time_t MIN_TS = 1697218894000; //FIXME: This is Friday Oct 13th 2023... Arbitrary?
const std::time_t MAX_TS = MIN_TS + ((int64_t) 86400000) * 50; //50 days in advance

// ----------------------------------------------------------------
// CACHE SIZES
// ----------------------------------------------------------------

 /** The number of FlightIds we want to keep cached locally at a client */
//TODO: CACHE_LIMIT_FLIGHT_IDS = 10000 //FIXME: Used for FindFlights

const int MAX_PENDING_INSERTS = 10000;
const int MAX_PENDING_UPDATES = 5000;
const int MAX_PENDING_DELETES = 5000;

// ----------------------------------------------------------------
// DATA SET INFORMATION
// ----------------------------------------------------------------

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
const int NUM_AIRPORTS = 9263;
const int NUM_COUNTRIES = 248;
const int NUM_AIRLINES = 1250;
}

#endif 