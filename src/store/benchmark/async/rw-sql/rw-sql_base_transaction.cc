/***********************************************************************
 *
 * Copyright 2025 Austin Li <atl63@cornell.edu>
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

#include "store/benchmark/async/rw-sql/rw-sql_base_transaction.h"

#include <fmt/core.h>

namespace rwsql {

const char ALPHA_NUMERIC[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
const uint64_t alpha_numeric_size = sizeof(ALPHA_NUMERIC) - 1; //Account for \0 terminator

static std::map<uint64_t, uint64_t> table_freq;
static std::map<uint64_t, uint64_t> base_freq;
static uint64_t periodic = 0;

RWSQLBaseTransaction::RWSQLBaseTransaction(QuerySelector *querySelector, uint64_t &numOps, std::mt19937 &rand, bool readSecondaryCondition, bool fixedRange, 
    int32_t value_size, uint64_t value_categories, bool readOnly, bool scanAsPoint, bool execPointScanParallel) 
    : rand(rand), querySelector(querySelector), numOps(numOps), readOnly(readOnly), readSecondaryCondition(readSecondaryCondition), 
    value_size(value_size), value_categories(value_categories), scanAsPoint(scanAsPoint), execPointScanParallel(execPointScanParallel), numKeys((int) querySelector->numKeys){

  max_random_size = value_categories < 0? UINT64_MAX : log(value_categories) / log(alpha_numeric_size);

  Notice("New TX with %d ops. Read only? %d", numOps, readOnly);

  for (int i = 0; i < numOps; ++i) { //Issue at least numOps many Queries
    uint64_t table = querySelector->tableSelector->GetKey(rand);  //Choose which table to read from for query i
    tables.push_back(table);

    uint64_t base = querySelector->baseSelector->GetKey(rand); //Choose which key to use as starting point for query i
    starts.push_back(base);

    if(false){
      Notice(" Next Scan. Table: %d. Base: %d.    Total num tables: %d. Total range: [0, %d]", table, base, querySelector->tableSelector->GetNumKeys(), querySelector->baseSelector->GetNumKeys());
      UW_ASSERT(table < querySelector->tableSelector->GetNumKeys());

      table_freq[table]++;
      base_freq[base]++;
      periodic++;
      if(periodic % 1000 == 0){
        Notice(" PRINTING TABLE FREQUENCIES:");
        for(auto &[tbl, freq]: table_freq){
          Notice("   Table: %d. Accessed: %d times", tbl, freq);
        }

        Notice(" PRINTING base FREQUENCIES:");
        for(auto &[base, freq]: base_freq){
          Notice("   Base: %d. Accessed: %d times", base, freq);
        }
      }
    }

    Debug(" Next Scan. Table: %d. Base: %d.    Total num tables: %d. Total range: [0, %d]", table, base, querySelector->tableSelector->GetNumKeys(), querySelector->baseSelector->GetNumKeys());



    bool is_point = std::uniform_int_distribution<uint64_t>(1, 100)(rand) <= querySelector->point_op_freq;

    if(is_point){
      ends.push_back(base);
    }
    else{ //do a scan
      //if using fixedRange => always pick the maxRange
      uint64_t range = fixedRange? querySelector->rangeSelector->GetNumKeys() - 1 : querySelector->rangeSelector->GetKey(rand); //Choose the number of keys to read (in addition to base) for query i
      uint64_t end = (base + range) % querySelector->numKeys; //calculate end point for range. Note: wrap around if > numKeys
      ends.push_back(end);
      Debug("Range: %d. Base: %d. End: %d", range, base, end);
    }

    secondary_values.push_back(GenerateSecondaryCondition());

  }
}

RWSQLBaseTransaction::~RWSQLBaseTransaction() {
}

std::pair<uint64_t, std::string> RWSQLBaseTransaction::GenerateSecondaryCondition(){
  std::pair<uint64_t, std::string> cond_pair = {0, ""};

  if(!readSecondaryCondition) return cond_pair;

  bool value_is_numeric = value_size < 0;

  if(value_is_numeric){
    //pick a random value within the categories;
    uint64_t upper_val = value_categories < 0? UINT64_MAX : value_categories;
    uint64_t val = std::uniform_int_distribution<size_t>(0, upper_val)(rand);
    cond_pair.first = val;
  }
  else{
    //make up to #FLAGS_value_categories many different strings
    //make the length = value_size.    Note: max categories = min(#alphanumeric^size, value_categories)
    //If no categories => just generate random string.
    std::string val;

     //Instead of random alpha numerics: pick category as int, and concat with a string.
    val = std::string(value_size, '0');
    uint64_t upper_val = value_categories < 0? UINT64_MAX : value_categories; //TODO: Replace UINT64_MAX with num_keys.
    uint64_t cat = std::uniform_int_distribution<size_t>(0, upper_val)(rand);
    //uint64_t cat = value_categories > 0 ? j  % value_categories : j ; //if no categories, pick i.
    val = std::to_string(cat) + val;
    val.resize(value_size);


    // for(int i = 0; i < value_size; ++i){
    //   if(i > max_random_size){ //if exhausted random categories.
    //     val += "0";
    //     continue;
    //   }
    //   int idx = std::uniform_int_distribution<size_t>(0, alpha_numeric_size-1)(rand);
    //   val += ALPHA_NUMERIC[idx];
    // }
    cond_pair.second = val;
  }
  return cond_pair;
}

void RWSQLBaseTransaction::ExecuteScanStatement(SyncClient &client, uint32_t timeout,
  const std::string &table_name, int &left_bound, int &right_bound,
  const std::pair<uint64_t, std::string> &cond_pair){

  std::string statement;


  if(POINT_READS_ENABLED && left_bound == right_bound){
    statement = fmt::format("SELECT * FROM {0} WHERE key = {1}", table_name, left_bound);
  }
  else if(left_bound <= right_bound){
    statement = fmt::format("SELECT * FROM {0} WHERE key >= {1} AND key <= {2}", table_name, left_bound, right_bound);
  } 
  else{
    statement = fmt::format("SELECT * FROM {0} WHERE key >= {1} OR key <= {2}", table_name, left_bound, right_bound);
  }

  if(readSecondaryCondition){
    //generate a random category.
    // auto cond_pair = GenerateSecondaryCondition();
    bool value_is_numeric = value_size < 0;
    if(value_is_numeric){
      statement += fmt::format(" AND value = {}", cond_pair.first);
    }
    else{
      statement += fmt::format(" AND value = '{}'", cond_pair.second);
    }
  }

  std::unique_ptr<const query_result::QueryResult> queryResult;
  client.Query(statement, queryResult, timeout);

  Debug("Query: %s found %d rows", statement.c_str(), queryResult->size());

  if(readOnly) return; //nothing more to do.

  //Otherwise, apply all the updates. //Note: Result already only includes those keys that meet the conditional.
  int num_res = queryResult->size();
  for(int i=0; i<num_res; ++i){
    uint64_t key;
    deserialize(key, queryResult, i, 0);
    Update(client, timeout, table_name, key, queryResult, i);
  }
}

//////////////////////////////// Point handling
void RWSQLBaseTransaction::ExecutePointStatements(SyncClient &client, uint32_t timeout, 
  const std::string &table_name, int &left_bound, int &right_bound,
  const std::pair<uint64_t, std::string> &cond_pair){

  // auto cond_pair = GenerateSecondaryCondition();

  std::string statement;
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  std::vector<int> idxs;

  auto idx = left_bound;
  while(idx != right_bound){
    idxs.push_back(idx);

    statement = fmt::format("SELECT * FROM {0} WHERE key = {1}", table_name, idx);

    Debug("Execute Query: %s", statement.c_str());

    if(execPointScanParallel){
      client.Query(statement, timeout);
    }
    else{
      client.Query(statement, queryResult, timeout);
      Debug("Finished Query: %s. Result size? %d", statement.c_str(), queryResult->size());
      ProcessPointResult(client, timeout, table_name, idx, queryResult, cond_pair);
    }
    idx = (idx + 1) % numKeys;
  }

  if(execPointScanParallel){
    client.Wait(results);
    UW_ASSERT(results.size() == idxs.size());
    for(int i=0; i < idxs.size(); ++ i){
      ProcessPointResult(client, timeout, table_name, idxs[i], results[i], cond_pair);
    }
  }

}

void RWSQLBaseTransaction::ProcessPointResult(SyncClient &client, uint32_t timeout,
    const std::string &table_name, const int &key, std::unique_ptr<const query_result::QueryResult> &queryResult, 
    const std::pair<uint64_t, std::string> &cond_pair){
  if(readOnly) return;

  try{

  if(readSecondaryCondition){
    //Check if value matches condition
    bool value_is_numeric = value_size < 0;
    if(value_is_numeric){
      uint64_t val;
      deserialize(val, queryResult, 0, 1); //TODO: Can we avoid duplicate deserialization here and after?
      Debug("Point read key: %d. val: %d", key, val);
      if(val != cond_pair.first) return;
    }
    else{
      std::string val;  
      deserialize(val, queryResult, 0, 1);
      Debug("Point read key: %d. val: %s", key, val.c_str());
      if(val != cond_pair.second) return;
      Debug("Succeeded condition for key: %d. val: %s", key, val.c_str());
    }
  }

  }
  catch(...){
    Panic("Debug deserialization");
  }


  Update(client, timeout, table_name, key, queryResult, 0);
}

void RWSQLBaseTransaction::Update(SyncClient &client, uint32_t timeout, 
  const std::string &table_name, const int &key, std::unique_ptr<const query_result::QueryResult> &queryResult, uint64_t row){
 //Note: this function can be used both for Scan/Point consumption.
  std::string statement;

  try{
  if(value_size < 0){ //using numeric val
    uint64_t val;
    deserialize(val, queryResult, row, 1);
    Debug("Read key: %d. val: %d", key, val);
    if(value_categories < 0){
      val+=1;
    }
    else{
      srand(val);
      val = std::rand() % value_categories;
    }
    statement = fmt::format("INSERT INTO {0} VALUES ({1}, {2})", table_name, key, val);
  }
  else{ //using string val
    //Create new random string using the current value as seed.
    std::string val;
    deserialize(val, queryResult, row, 1);
    Debug("Read key: %d. val: %s", key, val.c_str());
    std::hash<std::string> hasher;
    size_t hashValue = hasher(val);
    srand(hashValue);

    val.clear();
    val = std::string(value_size, '0');
    uint64_t cat = std::rand();
    if(value_categories >= 0) cat = cat % value_categories;
    val = std::to_string(cat) + val;
    val.resize(value_size);


    // for(int i = 0; i < value_size; ++i){
    //   if(i > max_random_size){ //if exhausted random categories.
    //     val += "0";
    //     continue;
    //   }
    //   int idx = std::rand() % alpha_numeric_size;
    //   val += ALPHA_NUMERIC[idx];
    // }
    statement = fmt::format("INSERT INTO {0} VALUES ({1}, '{2}')", table_name, key, val);
  }
  }
  catch(...){
    Panic("Debug crash in update logic");
  }

  Debug("Issue write: %s", statement.c_str());

  //Issue blind write. 
  std::unique_ptr<const query_result::QueryResult> dummyResult; //make this call synchronous -- we know because its a blind write it will return directly.
  client.Write(statement, dummyResult, timeout, true); //blind-write
}


//////////////////////////////////

std::string RWSQLBaseTransaction::GenerateStatement(const std::string &table_name, int &left_bound, int &right_bound){

  std::string statement;

  if(readOnly){
    if(POINT_READS_ENABLED && left_bound == right_bound) statement = fmt::format("SELECT * FROM {0} WHERE key = {1}", table_name, left_bound);
    if(left_bound <= right_bound) statement = fmt::format("SELECT * FROM {0} WHERE key >= {1} AND key <= {2}", table_name, left_bound, right_bound);
    else statement = fmt::format("SELECT * FROM {0} WHERE key >= {1} OR key <= {2}", table_name, left_bound, right_bound);
  }
  else{
    if(POINT_READS_ENABLED && left_bound == right_bound) statement = fmt::format("UPDATE {0} SET value = value + 1 WHERE key = {1};", table_name, left_bound);
    // if(left_bound == right_bound) query = fmt::format("UPDATE {0} SET value = value + 1 WHERE key = {1};", table, left_bound); // POINT QUERY -- TODO: FOR NOW DISABLE
    if(left_bound <= right_bound) statement = fmt::format("UPDATE {0} SET value = value + 1 WHERE key >= {1} AND key <= {2};", table_name, left_bound, right_bound);
    else statement = fmt::format("UPDATE {0} SET value = value + 1 WHERE key >= {1} OR key <= {2};", table_name, left_bound, right_bound); 
  }

  return statement;
}

void RWSQLBaseTransaction::SubmitStatement(SyncClient &client, uint32_t timeout,
  std::string &statement, const int &i){

  if(readOnly){
      if(PARALLEL_QUERIES){ 
        client.SQLRequest(statement, timeout);
        //client.Query(statement, timeout);
      }
      else{
        std::unique_ptr<const query_result::QueryResult> queryResult;
        client.SQLRequest(statement, queryResult, timeout); 
        //client.Query(statement, queryResult, timeout); 
        //TODO: Debug
      }
  }

  //Write queries.

  else{
  
    if(PARALLEL_QUERIES){ 
      // std::cerr << "Issue parallel Write request" << std::endl;
      Debug("Issue parallel Write request");
      client.SQLRequest(statement, timeout);  //client.Write
    }
    else{
      // std::cerr << "Issue sequential Write request" << std::endl;
      Debug("Issue sequential Write request");
      std::unique_ptr<const query_result::QueryResult> queryResult;
      client.SQLRequest(statement, queryResult, timeout);  //client.Write
    
      UW_ASSERT(queryResult->rows_affected());
      Debug("Num rows affected: %d",queryResult->rows_affected());
      // std::cerr << "Num rows affected: " << queryResult->rows_affected() << std::endl;

      int expected_size = (ends[i] - starts[i] + 1) % querySelector->numKeys;
      if(queryResult->rows_affected() < expected_size){ //ranges[i] + 1
        // std::cerr << "Was not able to read all expected rows -- Check whether initialized correctly serverside" << std::endl;
        Debug("Was not able to read all expected rows -- Check whether initialized correctly serverside");
        //TODO: if key doesn't exist => INSERT IT
        //Insert all -- just issue a bunch of point writes (bundle under one statement?) => TODO: Currently sql_interpreter does not support multi-writes
        //ideally just insert the missing ones, but we cannot tell WHICH ones are missing at the app layer -- queryResult only has num_rows_affected. //T
      }

      
    }
  }

}

void RWSQLBaseTransaction::GetResults(SyncClient &client, uint32_t timeout){

  if(PARALLEL_QUERIES){
    std::vector<std::unique_ptr<const query_result::QueryResult>> results; 
    client.Wait(results);

    for(auto &queryResult: results){


      if(!readOnly) UW_ASSERT(queryResult->rows_affected());
      // std::cerr << "Num rows affected: " << queryResult->rows_affected() << std::endl;
      Debug("Num rows affected: %d",queryResult->rows_affected() );
    }
  }

}


bool RWSQLBaseTransaction::AdjustBounds(int &left, int &right, uint64_t table,
  size_t &liveOps, std::vector<std::pair<int, int>> &past_ranges)
{

  // std::cerr << "ADJUSTING NEXT QUERY: " << std::endl;
  Debug("ADJUSTING NEXT QUERY:");
  //return false if statement is to be skipped.    
  // std::cerr << "Input Left: " << left << " Right: " << right << std::endl;

  //shrink in every loop (never grow!)
  for(auto [l, r]: past_ranges){
    // std::cerr << "Compare against: " << l << ":" << r << std::endl;
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
        // std::cerr << "dynamically lift l/r into higher plane" << std::endl;
        Debug("dynamically lift l/r into higher plane");
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
      // std::cerr << "case 1" << std::endl;
      return false;
    } 

    //3) encompass   left < l  r < right
    // => split
    if(left < l && r < right){
      // std::cerr << "case 3" << std::endl;
      //   std::cerr << "l: " << l << std::endl;
      //    std::cerr << "r: " << r << std::endl;
        liveOps+=2;
        // std::cerr << "SPLITTING INTO TWO. One: l: " << (left % numKeys) << "; r: " << ((l-1) %numKeys) << ". Two: l: " << ((r+1) %numKeys) << "; r: " << (right % numKeys) << std::endl;
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
    // std::cerr << "case 4" << std::endl;
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

  // std::cerr << "Output Left: " << left << " Right: " << right << std::endl;

  return true;
}


bool RWSQLBaseTransaction::AdjustBounds_manual(int &left, int &right, uint64_t table,
  size_t &liveOps, std::vector<std::pair<int, int>> &past_ranges)
{

  // std::cerr << "ADJUSTING NEXT QUERY: " << std::endl;
  //return false if statement is to be skipped.    
  // std::cerr << "Input Left: " << left << " Right: " << right << std::endl;

  UW_ASSERT(left < querySelector->numKeys && right < querySelector->numKeys);

  int size = wrap(right - left) + 1;

  //shrink in every loop (never grow!)
   for(auto &[l, r]: past_ranges){
      // std::cerr << "Comparing against past range: l="<< l << ", r=" << r << std::endl;
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
            // std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (wrap(l-1)) << ". Two: l: " << (wrap(r+1)) << "; r: " << right << std::endl;
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

          // std::cerr << "adjusted to Left: " << left << " Right: " << right << std::endl;

          
        }
        //Case A.2
        else
        { // right < left 

          //              l               r     right left      ==> split into two: left to l, r to right
          // right left   l               r                     ==> split into two: left to l, r to right
          if(right > r || left < l) {
            //  std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (wrap(l-1)) << ". Two: l: " << (wrap(r+1)) << "; r: " << right << std::endl;
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
            //  std::cerr << "SPLITTING INTO TWO. One: l: " << left << "; r: " << (wrap(l-1)) << ". Two: l: " << (wrap(r+1)) << "; r: " << right << std::endl;
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
      
      // std::cerr << "New left:" << left << std::endl;
      // std::cerr << "New right:" << right << std::endl;
           

      int new_size = wrap(right - left) + 1;
      // std::cerr << "size: " << size << std::endl;
      // std::cerr << "new size: " << new_size << std::endl;
      if(new_size > size) return false;  //Confirm that we shrank range (and not accidentally flipped signs and made it bigger)
      size = new_size;
  }

  // std::cerr << "Adjusted to Left: " << left << " Right: " << right << std::endl;
  UW_ASSERT(left < querySelector->numKeys && right < querySelector->numKeys);
  past_ranges.push_back({left, right});

  return true;
}

} // namespace rwsql
