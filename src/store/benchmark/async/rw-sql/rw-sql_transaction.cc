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
    : SyncTransaction(10000), querySelector(querySelector), numOps(numOps), readOnly(readOnly), numKeys((int) querySelector->numKeys){
  
  //std::cout << "New TX with numOps " << numOps << std::endl;
  for (int i = 0; i < numOps; ++i) { //Issue at least numOps many Queries
    uint64_t table = querySelector->tableSelector->GetKey(rand);  //Choose which table to read from for query i
    tables.push_back(table);

    uint64_t base = querySelector->baseSelector->GetKey(rand); //Choose which key to use as starting point for query i
    starts.push_back(base);

    uint64_t range = querySelector->rangeSelector->GetKey(rand); //Choose the number of keys to read (in addition to base) for query i
    uint64_t end = (base + range) % querySelector->numKeys; //calculate end point for range. Note: wrap around if > numKeys
    ends.push_back(end);
  }

  // starts.push_back(9);
  // ends.push_back(1);
  // starts.push_back(0);
  // ends.push_back(2);
  
}

RWSQLTransaction::~RWSQLTransaction() {
}

static int count = 2;

//WARNING: CURRENTLY DO NOT SUPPORT READ YOUR OWN WRITES
transaction_status_t RWSQLTransaction::Execute(SyncClient &client) {
  //Note: Semantic CC cannot help this Transaction avoid aborts. Since it does value++, all TXs that touch value must be totally ordered. 
  
  //reset Tx exec state. When avoiding redundant queries we may split into new queries. liveOps keeps track of total number of attempted queries
  liveOps = numOps;
  past_ranges.clear();
  statements.clear();


  Debug("Start next Transaction");
  //std::cerr << "Exec next TX" << std::endl;

  client.Begin(timeout);

  //Execute #liveOps queries
  for(int i=0; i < liveOps; ++i){
    std::cerr << "LiveOp: " << i << std::endl;
    std::cerr << "starts size: " << starts.size() << std::endl;
    //UW_ASSERT(liveOps <= (querySelector->numKeys)); //there should never be more ops than keys; those should've been cancelled. FIXME: new splits might only be cancelled later.

    string table_name = "table_" + std::to_string(tables[i]);
    int left_bound = starts[i]; 
    int right_bound = ends[i];  //If right_bound < left_bound, wrap around and read >= left, and <= right. Turn statement into OR
    UW_ASSERT(left_bound < querySelector->numKeys && right_bound < querySelector->numKeys);

    if(AVOID_DUPLICATE_READS){
      //adjust bounds: shrink to not overlap. //if shrinkage makes bounds invert => cancel this read.
      if(!AdjustBounds(left_bound, right_bound, tables[i])){
        std::cerr << "CANCELLED REDUNDANT QUERY" << std::endl;  
        continue;
      } 
    }
    UW_ASSERT(left_bound >= 0 && left_bound < querySelector->numKeys && right_bound >= 0 && right_bound < querySelector->numKeys);

    //std::string statement = GenerateStatement(table_name, left_bound, right_bound);
    statements.push_back(GenerateStatement(table_name, left_bound, right_bound));  
    std::string &statement = statements.back();

    Debug("Start new RW-SQL Request: %s", statement);
    std::cerr << "Start new RW-SQL Request: " << statement << std::endl;

    SubmitStatement(client, statement, i);
    //Note: Updates will not conflict on TableVersion -- Because we are not changing primary key, which is the search condition.  
  }

  GetResults(client);
 

    // client.Abort(timeout);
    // return ABORTED_USER;
  //if(++count == 3) Panic("stop testing");

  transaction_status_t commitRes = client.Commit(timeout);
  std::cerr << "TXN COMMIT STATUS: " << commitRes << std::endl;

  //usleep(1000);
  return commitRes;
}


std::string RWSQLTransaction::GenerateStatement(const std::string &table_name, int &left_bound, int &right_bound){

  if(readOnly){
    if(POINT_READS_ENABLED && left_bound == right_bound) return fmt::format("SELECT * FROM {0} WHERE key = {1};", table_name, left_bound);
    if(left_bound <= right_bound) return fmt::format("SELECT * FROM {0} WHERE key >= {1} AND key <= {2};", table_name, left_bound, right_bound);
    else return fmt::format("SELECT * FROM {0} WHERE key >= {1} OR key <= {2};", table_name, left_bound, right_bound);
  }
  else{
    if(POINT_READS_ENABLED && left_bound == right_bound) return fmt::format("UPDATE {0} SET value = value + 1 WHERE key = {1};", table_name, left_bound);
    // if(left_bound == right_bound) query = fmt::format("UPDATE {0} SET value = value + 1 WHERE key = {1};", table, left_bound); // POINT QUERY -- TODO: FOR NOW DISABLE
    if(left_bound <= right_bound) return fmt::format("UPDATE {0} SET value = value + 1 WHERE key >= {1} AND key <= {2};", table_name, left_bound, right_bound);
    else return fmt::format("UPDATE {0} SET value = value + 1 WHERE key >= {1} OR key <= {2};", table_name, left_bound, right_bound); 
  }
  
}

void RWSQLTransaction::SubmitStatement(SyncClient &client, std::string &statement, const int &i){

  if(readOnly){
      if(PARALLEL_QUERIES){ 
        client.Query(statement, timeout);
      }
      else{
        std::unique_ptr<const query_result::QueryResult> queryResult;
        client.Query(statement, queryResult, timeout); 
        //TODO: Debug
      }
  }

  //Write queries.

  else{
   
    if(PARALLEL_QUERIES){ 
      std::cerr << "Issue parallel Write request" << std::endl;
      client.Write(statement, timeout);
    }
    else{
      std::cerr << "Issue sequential Write request" << std::endl;
      std::unique_ptr<const query_result::QueryResult> queryResult;
      client.Write(statement, queryResult, timeout);  
    
      UW_ASSERT(queryResult->rows_affected());
      std::cerr << "Num rows affected: " << queryResult->rows_affected() << std::endl;

      int expected_size = (ends[i] - starts[i] + 1) % querySelector->numKeys;
      if(queryResult->rows_affected() < expected_size){ //ranges[i] + 1
        std::cerr << "Was not able to read all expected rows -- Check whether initialized correctly serverside" << std::endl;
        //TODO: if key doesn't exist => INSERT IT
        //Insert all -- just issue a bunch of point writes (bundle under one statement?) => TODO: Currently sql_interpreter does not support multi-writes
        //ideally just insert the missing ones, but we cannot tell WHICH ones are missing at the app layer -- queryResult only has num_rows_affected. //T
      }

      
    }
  }

}

void RWSQLTransaction::GetResults(SyncClient &client){

  if(PARALLEL_QUERIES){
    std::vector<std::unique_ptr<const query_result::QueryResult>> results; 
    client.Wait(results);

    for(auto &queryResult: results){
      if(!readOnly) UW_ASSERT(queryResult->rows_affected());
      std::cerr << "Num rows affected: " << queryResult->rows_affected() << std::endl;
    }
  }

}


bool RWSQLTransaction::AdjustBounds(int &left, int &right, uint64_t table)
{
  
  std::cerr << "ADJUSTING NEXT QUERY: " << std::endl;
  //return false if statement is to be skipped.    
  std::cerr << "Input Left: " << left << " Right: " << right << std::endl;

    //shrink in every loop (never grow!)
    for(auto [l, r]: past_ranges){
      std::cerr << "Compare against: " << l << ":" << r << std::endl;
      if(wrap(r-l)+1 == 0) return false; //previous query read everything.

      //Linearize all Keys onto a plane: 0 - (numKeys-1 + numKeys)  right side must always be >= left 
       if(l > r) r += numKeys;
       if(left > right) right += numKeys;

      //1) Check if no overlap: (in either plane) => if so, no adjustment necessary;
      //if(left > r || right < l) continue;

      //case a: both are in left plane:
      //case b: both are in right plane:
      if(left > r || right < l){
         //without adjusting planes, there is no overlap. Try moving l/r and left/right respectively.
        
        if(l < numKeys && r < numKeys){  // one of left/right must be in higher plane, so check for conflict there
         //case c: l/r are in left plane; 
          std::cerr << "dynamically lift l/r into higher plane" << std::endl;
          l += numKeys;
          r += numKeys;
          if(left > r || right < l) continue; //no overlap in higher plane either.
        }
        else if(left < numKeys && right < numKeys){ //one of l and r must already be in higher plane, so lift as well.
           //case d: left/right are in left plane;
          left += numKeys;
          right += numKeys;
          if(left > r || right < l) continue; //no overlap in higher plane either.
        }
      }

      
      //If any overlaps: Fine out which type of overlap: 
    

      //2) subsumed    l  <= left right <= r
      if(l <= left && right <= r){
        std::cerr << "case 1" << std::endl;
        return false;
      } 

      //3) encompass   left < l  r < right
      // => split
      if(left < l && r < right){
        std::cerr << "case 3" << std::endl;
          std::cerr << "l: " << l << std::endl;
           std::cerr << "r: " << r << std::endl;
          liveOps+=2;
          std::cerr << "SPLITTING INTO TWO. One: l: " << (left % numKeys) << "; r: " << ((l-1) %numKeys) << ". Two: l: " << ((r+1) %numKeys) << "; r: " << (right % numKeys) << std::endl;
          starts.push_back(left % numKeys);
          ends.push_back((l-1) % numKeys);
          tables.push_back(table);
          starts.push_back((r+1) % numKeys);
          ends.push_back(right % numKeys);
          tables.push_back(table);
          return false;
      }

      //4) overlap  l <= left <= r <= right   Or: left <= l <= right <= r
      //=> move out of bounds;
      std::cerr << "case 4" << std::endl;
      if(l <= left) left = r+1;  
      if(right <= r) right = l-1;

      //return range to original plane
      left = left % numKeys;
      right = right % numKeys;
    }

     //return range to original plane
    left = left % numKeys;
    right = right % numKeys;

    past_ranges.push_back({left, right});
    //if not spanning across planes, add to both planes:
    //if(left < numKeys && right < numKeys) past_ranges.push_back({left + numKeys, right + numKeys}); // => do it in-place instead

    std::cerr << "Output Left: " << left << " Right: " << right << std::endl;

    return true;
}


bool RWSQLTransaction::AdjustBounds_manual(int &left, int &right, uint64_t table)
{
  
  std::cerr << "ADJUSTING NEXT QUERY: " << std::endl;
  //return false if statement is to be skipped.    
  std::cerr << "Input Left: " << left << " Right: " << right << std::endl;

  UW_ASSERT(left < querySelector->numKeys && right < querySelector->numKeys);

    int size = wrap(right - left) + 1;

    //shrink in every loop (never grow!)
     for(auto &[l, r]: past_ranges){
        std::cerr << "Comparing against past range: l="<< l << ", r=" << r << std::endl;
        if(l <= r){
          if(r-l+1 == numKeys) return false;


          //Case A.1
          if(left <= right){
             //             l  left right   r                     ==> cancel
            if(l <= left && right <= r) return false;

              // left         l               r     right           ==> create 2: left to l, r to right
            if(left < l && r < right){
              //create two new updates instead!
              //Add them back to the queue; when we process them, we might have to shrink them again.
              std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (wrap(l-1)) << ". Two: l: " << (wrap(r+1)) << "; r: " << right << std::endl;
              liveOps+=2;
              starts.push_back(left);
              ends.push_back(wrap(l-1));
              tables.push_back(table);
              starts.push_back(wrap(r+1));
              ends.push_back(right);
              tables.push_back(table);
              return false;
            }

            //              l               r     left right      ==> do notihng
            // left right   l               r                     ==> do nothing
            if(left > r || right < l) continue;

            // left         l     right     r                     ==> shrink to right = l-1
            //              l      left     r     right           ==> shrink to left = r+1
            if(left < l) right = wrap(std::min(l-1, right)); //it must be that l <= right <= r
            if(right > r)  left = wrap(std::max(r+1, left)); //it must be that l <= left <= r
            //in both cases, make them non-overlapping.

            std::cerr << "adjusted to Left: " << left << " Right: " << right << std::endl;

            
          }
          //Case A.2
          else
          { // right < left 

            //              l               r     right left      ==> split into two: left to l, r to right
            // right left   l               r                     ==> split into two: left to l, r to right
            if(right > r || left < l) {
               std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (wrap(l-1)) << ". Two: l: " << (wrap(r+1)) << "; r: " << right << std::endl;
              liveOps+=2;
              starts.push_back(left);
              ends.push_back(wrap(l-1));
              tables.push_back(table);
              starts.push_back(wrap(r+1));
              ends.push_back(right);
              tables.push_back(table);
              return false;
            }

            // right        l               r     left          ==> do nothing
            //if(right < l && r < left) continue;
            //              l  right left   r                     ==> shrink left = r+1, right = l-1
            // right        l     left      r                     ==> shrink left = r+1
            //              l     right     r     left           ==> shrink to right = l-1
            left = wrap(std::max(r+1, left));
            right = wrap(std::min(l-1, right)); 
            //in all cases, just move left and right outside the l r range
           
          }
        }
        if(r < l){ //wrap around case
          if(r-l+1 == 0) return false;

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
            left = wrap(std::max(r+1, left));
            right = wrap(std::min(l-1, right));
            //in all cases, just move left and right inside the r l range

          }

          //Case B.2
          else{  //right < left
            
             // right       r               l     left          ==> cancel
            if(right <= r && l <= left) return false;

            //             r   right left  l
            if(r < right && left < l) {   
              //create two parallel reads: one from r to right, and one from left to l
               std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (wrap(l-1)) << ". Two: l: " << (wrap(r+1)) << "; r: " << right << std::endl;
              liveOps+=2;
              starts.push_back(left);
              ends.push_back(wrap(l-1));
              tables.push_back(table);
              starts.push_back(wrap(r+1));
              ends.push_back(right);
              tables.push_back(table);
              return false;
            }

           

            //in all other cases, just move left and right inside the r l range

            //             r               l     right left    ==> shrink to right = l-1 and left = r+1 
            // right left  r               l                   ==> shrink to right = l-1 and left = r+1    //this case.
            // right       r      left     l                   ==> shrink to right = l-1
            //             r     right     l     left          ==> shrink to left = r+1
            if(right <= r || right >= l){
              right = wrap(l-1);
            } 
            if(left <= r || left >= l){
              left = wrap(r+1);
            } 

            
           
            // //             r               l     right left    ==> shrink to right = l-1 and left = r+1 
            // // right left  r               l                   ==> shrink to right = l-1 and left = r+1 
            // if(l <= right || left <= r){
            //    right = wrap(std::min(l-1, right));
            //   std::cerr << "New right:" << right << std::endl;
            //   left = wrap(std::max(r+1, left));
            // }


            //  // right       r      left     l                   ==> shrink to right = l-1
            //  if(right <= r){ //&& left inbetween
            //     right = wrap(l-1);
            //  }

            // //             r     right     l     left          ==> shrink to left = r+1
            // if(left >= l){ //&& right inbetween
            //     left = wrap(r+1);
            // }

           
            

          }
        }
        
        std::cerr << "New left:" << left << std::endl;
        std::cerr << "New right:" << right << std::endl;
             

        int new_size = wrap(right - left) + 1;
        std::cerr << "size: " << size << std::endl;
        std::cerr << "new size: " << new_size << std::endl;
        if(new_size > size) return false;  //Confirm that we shrank range (and not accidentally flipped signs and made it bigger)
        size = new_size;
    }

    std::cerr << "Adjusted to Left: " << left << " Right: " << right << std::endl;
    UW_ASSERT(left < querySelector->numKeys && right < querySelector->numKeys);
    past_ranges.push_back({left, right});

    return true;
}


} // namespace rw
