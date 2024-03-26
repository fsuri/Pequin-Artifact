#include <fstream>
#include <iostream>

#include "store/cockroachdb/client.h"

using namespace std;

#define TIMEOUT 1

// Reply types
#define REPLY_OK 0
#define REPLY_FAIL 1
#define REPLY_RETRY 2
#define REPLY_ABSTAIN 3
#define REPLY_TIMEOUT 4
#define REPLY_NETWORK_FAILURE 5
#define REPLY_MAX 6
#define UNKNOWN 69

int main(int argc, char** argv) {
  // define callbacks & related value
  string obs_str = "";
  int obs_sts = UNKNOWN;

  // Begin
  begin_callback bcb = [](int _) { cout << "Tx begins" << endl; };
  begin_timeout_callback btcb = []() {};

  // Put
  put_callback pcb = [&obs_sts](int pstatus, string k, string v) {
    obs_sts = pstatus;
  };
  put_timeout_callback ptcb = [](int _, string k, string v) {};

  // Get
  get_callback gcb = [&obs_sts, &obs_str](int gstatus, string key, string val,
                                          Timestamp ts) {
    obs_sts = gstatus;
    obs_str = val;
  };
  get_timeout_callback gtcb = [](int _, string k) {};

  // Query
  query_callback qcb = [&obs_sts, &obs_str](int qstatus,
                                            tao::pq::result result) {
    obs_sts = qstatus;
    obs_str = result.as<string>();
  };
  query_timeout_callback qtcb = [](int _) {};

  // Commit
  commit_callback ccb = [&obs_sts](transaction_status_t cstatus) {
    obs_sts = cstatus;
    cout << "Tx committed with status:" << cstatus << endl;
  };
  commit_timeout_callback ctcb = []() {};
  abort_callback acb = []() { cout << "Tx aborted" << endl; };
  abort_timeout_callback atcb = []() {};

  // Initialize a client
  cockroachdb::Client* client;
  try {
    client = new cockroachdb::Client(argv[1]);
  } catch (const std::exception& e) {
    std::cerr << "Unable to read test.config: " << '\n';
    std::cerr << e.what() << '\n';
  }
  // Begin a Tx
  client->Begin(bcb, btcb, TIMEOUT, false);

  // Put something
  client->Put("4410", "OS", pcb, ptcb, TIMEOUT);
  assert(obs_sts == 0);

  // Put something else
  obs_sts = UNKNOWN;
  client->Put("4820", "Typo", pcb, ptcb, TIMEOUT);
  assert(obs_sts == 0);

  // Put and Get (READ UNCOMITTED)
  client->Get("4820", gcb, gtcb, TIMEOUT);
  cout << "Expected: Typo - "
       << "Got: " << obs_str << endl;
  assert(obs_str == "Typo");
  client->Commit(ccb, ctcb, TIMEOUT);

  // Put and abort
  client->Begin(bcb, btcb, TIMEOUT, false);
  client->Put("4820", "Algo", pcb, ptcb, TIMEOUT);
  client->Abort(acb, atcb, TIMEOUT);

  client->Get("4820", gcb, gtcb, TIMEOUT);
  cout << "Expected: Typo - "
       << "Got: " << obs_str << endl;
  assert(obs_str == "Typo");

  // Put (overwrite) and commit
  client->Begin(bcb, btcb, TIMEOUT, false);
  client->Put("4820", "Algo", pcb, ptcb, TIMEOUT);
  client->Get("4820", gcb, gtcb, TIMEOUT);
  cout << "Expected: Algo - "
       << "Got: " << obs_str << endl;
  client->Commit(ccb, ctcb, TIMEOUT);

  assert(obs_str == "Algo");
  client->conn->execute("DROP TABLE datastore");
  return 0;
}