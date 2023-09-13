/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#include "store/benchmark/async/rw-sql/rw-sql_transaction.h"
#include <fmt/core.h>
#include "store/common/query_result/query_result.h"

namespace rwsql {


RWSQLTransaction::RWSQLTransaction(QuerySelector *querySelector, uint64_t &numOps, std::mt19937 &rand, bool readOnly) 
    : SyncTransaction(10000), querySelector(querySelector), numOps(numOps), readOnly(readOnly) {

  //std::cout << "New TX with numOps " << numOps << std::endl;
  for (int i = 0; i < numOps; ++i) {
    uint64_t table = querySelector->tableSelector->GetKey(rand);   //TODO: This will pick 0 or 1?
    tables.push_back(table);

    uint64_t base = querySelector->baseSelector->GetKey(rand);
    bases.push_back(base);

    uint64_t range = querySelector->rangeSelector->GetKey(rand); 
    ranges.push_back(range);
  }
  
}

RWSQLTransaction::~RWSQLTransaction() {
}


transaction_status_t RWSQLTransaction::Execute(SyncClient &client) {
  
  client.Begin(timeout);

 //RW LOGIC
  //UPDATE / INSERT / READ
  for(int i=0; i < numOps; ++i){
    
    string table = "table_" + std::to_string(tables[i]);
    int left_bound = 7; //bases[i]; 
    //std::cout << "left: " << left_bound << std::endl;
    int right_bound = 3; //(left_bound + ranges[i]) % querySelector->numKeys;   //If keys+ range goes out of bound, wrap around and check smaller and greaer. Turn statement into OR
    //std::cout << "range " << ranges[i] << std::endl;
    //std::cout << "numKeys " << querySelector->numKeys << std::endl;
  
    std::string statement;    
    
    if(readOnly){
      if(left_bound <= right_bound) statement = fmt::format("SELECT FROM {0} WHERE key >= {1} AND key <= {2};", table, left_bound, right_bound);
      else statement = fmt::format("SELECT FROM {0} WHERE key >= {1} OR key <= {2};", table, left_bound, right_bound);
    }

    else{
      // if(left_bound == right_bound) query = fmt::format("UPDATE {0} SET value = value + 1 WHERE key = {1};", table, left_bound); // POINT QUERY -- TODO: FOR NOW DISABLE
      if(left_bound <= right_bound) statement = fmt::format("UPDATE {0} SET value = value + 1 WHERE key >= {1} AND key <= {2};", table, left_bound, right_bound);
      else statement = fmt::format("UPDATE {0} SET value = value + 1 WHERE key >= {1} OR key <= {2};", table, left_bound, right_bound); 

    }
    //TODO: FIXME: Currently Ignoring TableVersion writes -- Because we KNOW that we are not changing primary key, which is the search condition.  


    Debug("Start new RW-SQL Request: %s", statement);
    std::cerr << "Start new RW-SQL Request: " << statement << std::endl;
           
    
    if(readOnly){
        std::unique_ptr<const query_result::QueryResult> queryResult;
        client.Query(statement, queryResult, timeout);  //--> Edit API in frontend sync_client.
                                           //For real benchmarks: Also edit in sync_transaction_bench_client.
    }
    else{
      std::unique_ptr<const query_result::QueryResult> queryResult;
      client.Write(statement, queryResult, timeout);  //--> Edit API in frontend sync_client.
                                           //For real benchmarks: Also edit in sync_transaction_bench_client.
    
      //TODO: if key doesn't exist => INSERT IT
      UW_ASSERT(queryResult->rows_affected());
      std::cerr << "Expected rows affected. Max: " << 3 << std::endl;
      std::cerr << "Num rows affected: " << queryResult->rows_affected() << std::endl;

      if(queryResult->rows_affected() < ranges[i] + 1){
        std::cerr << "Was not able to read all expected rows -- Check whether initialized correctly serverside" << std::endl;
        //Insert all -- just issue a bunch of point writes (bundle under one statement?) => TODO: check if sql_interpreter deals with multi-writes
        //ideally just insert the missing ones, but we don't know.
      }
    }
   
                              
    //sleep(9);
  
  }

    // client.Abort(timeout);
    // return ABORTED_USER;
  
  transaction_status_t commitRes = client.Commit(timeout);
  return commitRes;
}

} // namespace rw
