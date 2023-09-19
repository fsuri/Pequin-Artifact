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
#include "rw-sql_transaction.h"

namespace rwsql {


RWSQLTransaction::RWSQLTransaction(QuerySelector *querySelector, uint64_t &numOps, std::mt19937 &rand, bool readOnly) 
    : SyncTransaction(10000), querySelector(querySelector), numOps(numOps), readOnly(readOnly) {

  //std::cout << "New TX with numOps " << numOps << std::endl;
  for (int i = 0; i < numOps; ++i) {
    uint64_t table = querySelector->tableSelector->GetKey(rand);   //TODO: This will pick 0 or 1?
    tables.push_back(table);

    uint64_t base = querySelector->baseSelector->GetKey(rand);
    starts.push_back(base);

    uint64_t range = querySelector->rangeSelector->GetKey(rand); 
    uint64_t end = (base + range) % querySelector->numKeys;
    ends.push_back(end);
  }
  
}

RWSQLTransaction::~RWSQLTransaction() {
}



transaction_status_t RWSQLTransaction::Execute(SyncClient &client) {
  
  //reset Tx exec state
  liveOps = numOps;
  past_ranges.clear();


  //CURRENTLY DO NOT SUPPORT READ YOUR OWN WRITES
    //Could simulate within TX by making it +2 for the subsumed ends.

  std::cerr << "Exec next TX" << std::endl;

  client.Begin(timeout);

  std::cerr << "Begin TX" << std::endl;
 //RW LOGIC
  //UPDATE / INSERT / READ
  for(int i=0; i < liveOps; ++i){
    
    string table = "table_" + std::to_string(tables[i]);
    uint64_t left_bound = starts[i]; 
    std::cout << "left: " << left_bound << std::endl;
    uint64_t right_bound = ends[i]; //(left_bound + ends[i]) % querySelector->numKeys;   //If keys+ range goes out of bound, wrap around and check smaller and greaer. Turn statement into OR
    std::cout << "right: " << right_bound << std::endl;
    //std::cout << "range " << ends[i] << std::endl;
    //std::cout << "numKeys " << querySelector->numKeys << std::endl;
  
    if(AVOID_DUPLICATE_READS){
      //adjust bounds: shrink to not overlap. //if shrinkage makes bounds invert => cancel this read.
      UW_ASSERT(left_bound < querySelector->numKeys && right_bound < querySelector->numKeys);
      if(!AdjustBounds(left_bound, right_bound, tables[i])){
        std::cerr << "CANCELLED REDUNDANT QUERY" << std::endl;   //TODO: Run Split Queries in parallel. Use Wait interface.
        continue;
      } 
      UW_ASSERT(left_bound < querySelector->numKeys && right_bound < querySelector->numKeys);
    }

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
      std::cerr << "send Query TX" << i << std::endl;
      std::unique_ptr<const query_result::QueryResult> queryResult;
      client.Write(statement, queryResult, timeout);  //--> Edit API in frontend sync_client.
                                           //For real benchmarks: Also edit in sync_transaction_bench_client.
    
      //TODO: if key doesn't exist => INSERT IT
      UW_ASSERT(queryResult->rows_affected());
      std::cerr << "Expected rows affected. Max: " << 3 << std::endl;
      std::cerr << "Num rows affected: " << queryResult->rows_affected() << std::endl;

      if(queryResult->rows_affected() < ends[i] + 1){
        std::cerr << "Was not able to read all expected rows -- Check whether initialized correctly serverside" << std::endl;
        //Insert all -- just issue a bunch of point writes (bundle under one statement?) => TODO: check if sql_interpreter deals with multi-writes
        //ideally just insert the missing ones, but we don't know.
      }
    }
  }

    // client.Abort(timeout);
    // return ABORTED_USER;
  
  transaction_status_t commitRes = client.Commit(timeout);
  std::cerr << "TXN COMMIT STATUS: " << commitRes << std::endl;

  //usleep(1000);
  return commitRes;
}


bool RWSQLTransaction::AdjustBounds(uint64_t &left, uint64_t &right, uint64_t table)
{
  
  std::cerr << "ADJUSTING NEXT QUERY: " << std::endl;
  //return false if statement is to be skipped.    
  std::cerr << "Input Left: " << left << " Right: " << right << std::endl;

  UW_ASSERT(left < querySelector->numKeys && right < querySelector->numKeys);

    int size = (right - left + 1) % querySelector->numKeys;

    //shrink in every loop (never grow!)
     for(auto &[l, r]: past_ranges){
        std::cerr << "Comparing against past range: l="<< l << ", r=" << r << std::endl;
        if(l <= r){
          //Case A.1
          if(left <= right){
             //             l  left right   r                     ==> cancel
            if(l <= left && right <= r) return false;

              // left         l               r     right           ==> create 2: left to l, r to right
            if(left < l && r < right){
              //create two new updates instead!
              //Add them back to the queue; when we process them, we might have to shrink them again.
              std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (l-1) << ". Two: l: " << (r+1) << "; r: " << right << std::endl;
              liveOps+=2;
              starts.push_back(left);
              ends.push_back((l-1) % querySelector->numKeys);
              tables.push_back(table);
              starts.push_back((r+1) % querySelector->numKeys);
              ends.push_back(right);
              tables.push_back(table);
              return false;
            }

            //              l               r     left right      ==> do notihng
            // left right   l               r                     ==> do nothing
            if(left > r || right < l) continue;

            // left         l     right     r                     ==> shrink to right = l-1
            //              l      left     r     right           ==> shrink to left = r+1
            if(left < l) right = std::min(l-1, right) % querySelector->numKeys; //it must be that l <= right <= r
            if(right > r)  left = std::max(r+1, left) % querySelector->numKeys; //it must be that l <= left <= r
            //in both cases, make them non-overlapping.

            std::cerr << "adjusted to Left: " << left << " Right: " << right << std::endl;

            
          }
          //Case A.2
          else
          { // right < left 

            //              l               r     right left      ==> split into two: left to l, r to right
            // right left   l               r                     ==> split into two: left to l, r to right
            if(right > r || left < l) {
              std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (l-1) << ". Two: l: " << (r+1) << "; r: " << right << std::endl;
              liveOps+=2;
              starts.push_back(left);
              ends.push_back((l-1) % querySelector->numKeys);
              tables.push_back(table);
              starts.push_back((r+1) % querySelector->numKeys);
              ends.push_back(right);
              tables.push_back(table);
              return false;
            }

            // right        l               r     left          ==> do nothing
            //if(right < l && r < left) continue;
            //              l  right left   r                     ==> shrink left = r+1, right = l-1
            // right        l     left      r                     ==> shrink left = r+1
            //              l     right     r     left           ==> shrink to right = l-1
            left = std::max(r+1, left) % querySelector->numKeys;
            right = std::min(l-1, right) % querySelector->numKeys; 
            //in all cases, just move left and right outside the l r range
           
          }
        }
        if(r < l){ //wrap around case
          //Case B.1
          if(left <= right){
           
            //              r   left right  l              ==> do nothing
            if(r < left && right < l) continue;

            //              r               l     left right      ==> cancel
            // left right   r               l                     ==> cancel
            if(left >= l || right <= r ) return false;
          
            // left         r               l     right           ==> shrink to  left = r+1, right = l-1
            // left         r     right     l                     ==> shrink to left = r+1
            //              r      left     l     right           ==> shrink to right = l-1
            left = std::max(r+1, left) % querySelector->numKeys;
            right = std::min(l-1, right) % querySelector->numKeys;
            //in all cases, just move left and right inside the r l range

          }

          //Case B.2
          else{  //right < left
            
            //             r   right left  l
            if(right > r && left < r) {   
              //create two parallel reads: one from r to right, and one from left to l
              std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (l-1) << ". Two: l: " << (r+1) << "; r: " << right << std::endl;
              liveOps+=2;
              starts.push_back(left);
              ends.push_back((l-1) % querySelector->numKeys);
              tables.push_back(table);
              starts.push_back((r+1) % querySelector->numKeys);
              ends.push_back(right);
              tables.push_back(table);
              return false;
            }


            // right       r               l     left          ==> cancel
            if(right <= r && l <= left) return false;

            
            // right       r      left     l                   ==> shrink to right = l-1
            //             r     right     l     left          ==> shrink to left = r+1
            //             r               l     right left    ==> shrink to right = l-1 and left = r+1 
            // right left  r               l                   ==> shrink to right = l-1 and left = r+1 
            right = std::min(l-1, right) % querySelector->numKeys;
            left = std::max(r+1, left) % querySelector->numKeys;
            //in all cases, just move left and right inside the r l range

          }
        }

        int new_size = (right - left + 1) % querySelector->numKeys;
        if(new_size > size) return false;  //Confirm that we shrank range (and not accidentally flipped signs and made it bigger)
        size = new_size;
    }

    std::cerr << "Adjusted to Left: " << left << " Right: " << right << std::endl;
    UW_ASSERT(left < querySelector->numKeys && right < querySelector->numKeys);
    past_ranges.push_back({left, right});

    return true;
}


} // namespace rw
