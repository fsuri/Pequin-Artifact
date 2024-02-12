#include <vector> 
#include "store/benchmark/async/sql/tpcc/tpcc_schema.h"

namespace tpcch_sql {

const std::vector<int> DEFAULT_Q_WEIGHTS = {3, 2, 3, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5};

const int NUM_SUPPLIERS = 10000;

/** % that client in tpcch benchmark is a TPCCH client, and not TPCC*/
const double PROB_TPCCH_CLIENT = 0.5;     


const std::string REGION_TABLE = "region";
const std::string NATION_TABLE = "nation";
const std::string SUPPLIER_TABLE = "supplier";


}