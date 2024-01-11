#include <vector> 

namespace tpcch_sql {

const std::vector<int> DEFAULT_Q_WEIGHTS = {3, 2, 3, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5};

const int NUM_SUPPLIERS = 10000;

/** % that client in tpcch benchmark is a TPCCH client, and not TPCC*/
const double PROB_TPCCH_CLIENT = 0.5;     
}