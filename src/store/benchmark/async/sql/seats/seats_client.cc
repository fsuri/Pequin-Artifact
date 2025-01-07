#include "store/benchmark/async/sql/seats/seats_client.h"
#include "store/benchmark/async/sql/seats/delete_reservation.h"
#include "store/benchmark/async/sql/seats/find_flights.h"
#include "store/benchmark/async/sql/seats/find_open_seats.h"
#include "store/benchmark/async/sql/seats/new_reservation.h" 
#include "store/benchmark/async/sql/seats/update_customer.h"
#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

#include <cmath>
#include <queue>



namespace seats_sql {


SEATSSQLClient::SEATSSQLClient(SyncClient &client, Transport &transport, const std::string &profile_file_path, uint32_t scale_factor,
      uint64_t id, uint64_t numRequests, uint64_t expDuration, uint64_t delay, uint64_t warmupSec,
      uint64_t cooldownSec, uint64_t tputInterval, uint32_t abortBackoff, bool retryAborted, 
      uint64_t maxBackoff, int64_t maxAttempts,
      uint64_t timeout, const std::string &latencyFilename) :       
      SyncTransactionBenchClient(client, transport, id, numRequests,
        expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename), profile(id, scale_factor, gen) { 
            gen.seed(id);
            started_workload = false;


     profile.LoadProfile(profile_file_path);
}

SEATSSQLClient::~SEATSSQLClient() {}

SyncTransaction* SEATSSQLClient::GetNextTransaction() {

  // need to populate reservations first
  if (!started_workload) {
    started_workload = true; 
    return new SQLFindOpenSeats(GetTimeout(), gen, profile);
  }

  // keep going until we get a valid operation
  while (true) {
    int64_t t_type = std::uniform_int_distribution<int64_t>(1, 100)(gen);
    Debug("NEXT T_TYPE: %d", t_type);
    int freq = 0;
    if (t_type <= (freq = FREQUENCY_DELETE_RESERVATION)) {
      Debug("Try Delete_Res. Is empty? %d", profile.delete_reservations.empty());
      if (profile.delete_reservations.empty()) 
        continue;
      last_op_ = "delete_reservation";
      return new SQLDeleteReservation(GetTimeout(), gen, profile);
    } 
    else if (t_type <= (freq += FREQUENCY_FIND_FLIGHTS)) {
      last_op_ = "find_flight";
      return new SQLFindFlights(GetTimeout(), gen, profile); 
    } 
    else if (t_type <= (freq += FREQUENCY_FIND_OPEN_SEATS)) {
      last_op_ = "find_open_seats";
      return new SQLFindOpenSeats(GetTimeout(), gen, profile);
    } 
    else if (t_type <= (freq += FREQUENCY_NEW_RESERVATION)) {
      if (profile.insert_reservations.empty())
        continue; 
      last_op_ = "new_reservation";
      int64_t r_id = ((int64_t) profile.seats_id | (profile.num_res_made++) << 32);
      return new SQLNewReservation(GetTimeout(), gen, r_id, profile);
    } 
    else if (t_type <= (freq += FREQUENCY_UPDATE_CUSTOMER)) {
      last_op_ = "update_customer";
      return new SQLUpdateCustomer(GetTimeout(), gen, profile);
    } 
    else {
      Debug("Try Update_Res. Is empty? %d", profile.update_reservations.empty()); 
      if (profile.update_reservations.empty())
        continue;
      last_op_ = "update_reservation";
      return new SQLUpdateReservation(GetTimeout(), gen, profile);
    }
  }
}

std::string SEATSSQLClient::GetLastOp() const {
  return last_op_;
}

} 
