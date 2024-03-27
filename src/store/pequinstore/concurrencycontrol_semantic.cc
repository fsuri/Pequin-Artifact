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


#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>


#include "store/pequinstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include <sstream>
#include <list>
#include <utility>
#include <string_view>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/pequinstore/common.h"
#include "store/pequinstore/phase1validator.h"
#include "store/pequinstore/localbatchsigner.h"
#include "store/pequinstore/sharedbatchsigner.h"
#include "store/pequinstore/basicverifier.h"
#include "store/pequinstore/localbatchverifier.h"
#include "store/pequinstore/sharedbatchverifier.h"
#include "store/pequinstore/query-engine/optimizer/plan_generator.h"
#include "store/pequinstore/query-engine/type/type.h"
#include "store/pequinstore/query-engine/type/value.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

namespace pequinstore {

static bool lazy_check = true; //TURN TO FALSE TO ENABLE RECHECK FOR MORE PRECISE DEP DISCOVERY (has possibly redundant overhead though)

//DEPRECATED: Using proto::ReadPredicate
// // The Predicate that we store locally.
// struct ReadPredicate {
//   std::string table_name;
//   Timestamp table_version;
//   //TODO: Start out without col version. Only add as potential refinement/optimization (reduce number of comparisons..)
//   std::vector<std::string> col_names;
//   std::vector<Timestamp> col_versions;
//   std::string where_clause;   
//   //NOTE: RE EVALUATE: If we ever move to delta encoding for TableWrites, then we must create "placeholder values for remaining cols of tuple when evaluating"
  
//   //For nested join: We might not want to store 100 predicates if there are 100 right loop iterations. => store where_clause with some placeholders {} (just use fmt::format to fill in)
//   std::vector<std::vector<<std::string> fill_values; // (e.g. 5) - one vector per loop iteration, and in each loop iteration, one entry per col that needs replacement.
  
//   //Some reference to owner TX? Maybe TS sufficces?
//   Timestamp txn_ts; //Might not even be needed here if we store a map: name -> map<TS, pred> 
//   // proto::Transaction *txn;
//   // std::string txn_digest;
        
//   bool commit_or_prepare; //1 if commit, 0 if prepare
// };


//NOTE: Table/Col Versions continue to be stored in the read and write set but have to CC functionality. They are purely used as a simple hack to acquire scoped locks for CC

//TODO: Read Predicate Caching
//(During CC merge them (or just loop through all of them..))       
//If we cache: Different replica may have different predicates (for nested joins) (Note: for flat queries will always be the same)
                              // Include hash, and vote abort if different form hash. (Note: With Snapshot mode all will have same pred (bar committed overrides of materialized ss).)


//TODO: If we want to support Column Versions Extend TableWrite with set of columns touched + mark each row as (insert/update/delete)
//TODO: Technically need to verify whether client includes all Col Versions that it should. To do so, must either include Query, or check that only those cols are being updated
      //This is much easier to enforce if TableWrites only include deltas for the updated cols.


//Enforce that we can ony issue monotonic writes
////TODO: Alternatively: Could enforce that we can't see any writes older than the newest table version observed in a prepared/committed pred. 
          //To check this would need a third map storing the high read TableVersion.
bool Server::CheckMonotonicTableColVersions(const std::string &txn_digest, const proto::Transaction &txn) {
  for(auto &[table_name, _]: txn.table_writes()){
     //Get Last version on this table                      
    TableWriteMap::const_accessor tw;
    if(!tableWrites.find(tw, table_name)) continue;
    if(tw->second.rbegin() == tw->second.rend()) continue;
    const Timestamp &highTS = tw->second.rbegin()->first; // == last TX TS
    
    //NOTE: only comparing on the real time component currently.
    if(txn.timestamp().timestamp() + write_monotonicity_grace <= highTS.getTimestamp()){
      Warning("Aborting txn: %s. Non monotonic Table/Col Write to [%s]! ms_diff: %lu [ts_diff: %lu]. ms_grace: %lu [ts_grace: %lu]. writeTxnTS: %lu < highTS: %lu", 
          BytesToHex(txn_digest, 16).c_str(), table_name.c_str(), 
          timeServer.TStoMS(highTS.getTimestamp() - txn.timestamp().timestamp()), highTS.getTimestamp() - txn.timestamp().timestamp(), //diffs
          params.query_params.monotonicityGrace,  write_monotonicity_grace,  //grace
          txn.timestamp().timestamp(), highTS.getTimestamp());

      return false;
    } 
  }
  return true;
}

//NOTE: This must be called before waiting for dependencies. The pred check might dynamically add some deps...
//TODO: don't add these deps to txn. add them somewhere else to check...? Then it's tricky how to wake up. 
//Easiest is to add them to the TXN. But then the TXN doesn't fulfill Hash anymore...
    //Need to store them to some extra merged_set, and include them on demand for dep checks (and wakeup dep checks)

proto::ConcurrencyControl::Result Server::CheckPredicates(const proto::Transaction &txn, const Timestamp &txn_ts, const ReadSet &txn_read_set, const PredSet &pred_set, std::set<std::string> &dynamically_active_dependencies){ 
  
    Debug("Checking for Semantic Conflicts. TX_ts: [%lu:%lu]. Num preds: %d. Num TableWrites: %d", txn_ts.getTimestamp(), txn_ts.getID(), pred_set.size(), txn.table_writes().size());
   
    //For all Read predicates     
   
    for(auto &pred: pred_set){ //Note: there will be #preds for the query == number of tables in the SQL query 
          // => Check whether they are invalidated by any recent write to the Table
        auto res = CheckReadPred(txn_ts, pred, txn_read_set, dynamically_active_dependencies);
        if(res != proto::ConcurrencyControl::COMMIT) return res;
          //TODO: Return ABORT if conflict is with committed (in this case, must pass a CommitProof too) (Note: we don't currently support verification of these at client)
    }
    
                
    //For all (Table) Writes 
    for(auto &[table_name, table_write]: txn.table_writes()){
       // => Check whether they are invalidated by any recently prepared/committed read predicate on the Table
        auto res = CheckTableWrites(txn, txn_ts, table_name, table_write); //0 = no conflict, 1 = conflict with commit, 2 = conflict with prepared.
        if(res != proto::ConcurrencyControl::COMMIT) return res;
          //TODO: Return ABORT if conflict is with committed (in this case, must pass a CommitProof too) (Note: we don't currently support verification of these at client)
    }
  
    return proto::ConcurrencyControl::COMMIT;
}

//DEPRECATED
proto::ConcurrencyControl::Result Server::CheckPredicates(const proto::Transaction &txn, const ReadSet &txn_read_set, std::set<std::string> &dynamically_active_dependencies){ 
  //TODO: Support also passing via Hashing? (Either find ReadPredicates on demand here, or in advance)
        //Should be enough to do it on demand here, since we don't need them to lock if we add them to the read set?
    const Timestamp txn_ts(txn.timestamp());

   
    //For all Read predicates               //TODO: Ideally, organize read predicates by Table as well, so we don't loop over all tables in bounded history each time?
    for(auto &query_md : txn.query_set()){
       UW_ASSERT(query_md.group_meta().count(groupIdx));
      const proto::QueryGroupMeta &group_meta = query_md.group_meta().at(groupIdx); //only need to look at the pred for partitions this group is responsible for (Note: we don't support partitions currently)

      const proto::ReadSet &read_set = group_meta.query_read_set();
      //TODO: If cached: get read predicates.
      for(auto &pred: read_set.read_predicates()){ //Note: there will be #preds for the query == number of tables in the SQL query 
           // => Check whether they are invalidated by any recent write to the Table
          auto res = CheckReadPred(txn_ts, pred, txn_read_set, dynamically_active_dependencies);
          if(res != proto::ConcurrencyControl::COMMIT) return res;
            //TODO: Return ABORT if conflict is with committed (in this case, must pass a CommitProof too) (Note: we don't currently support verification of these at client)
      }
    }
   
     
    //For all (Table) Writes 
    for(auto &[table_name, table_write]: txn.table_writes()){
       // => Check whether they are invalidated by any recently prepared/committed read predicate on the Table
        auto res = CheckTableWrites(txn, txn_ts, table_name, table_write); //0 = no conflict, 1 = conflict with commit, 2 = conflict with prepared.
        if(res != proto::ConcurrencyControl::COMMIT) return res;
          //TODO: Return ABORT if conflict is with committed (in this case, must pass a CommitProof too) (Note: we don't currently support verification of these at client)
    }
  
    return proto::ConcurrencyControl::COMMIT;
}


std::string Server::GetEncodedRow(const proto::Transaction &txn, const RowUpdates &row, const std::string &table_name){

  //ALTERNATIVELY:
  //Instead of looping over table rows and getting the encoded write key, we could also loop over the write keys directly, and get the row via the index
  //Note: We'd still somehow need access to the table in question (for CheckRead preds its easy, we already have it passed, but for CheckWrites it's annoying)

   //NOTE: We now Correct the write set idx in case of sorting! So the on demand encoding is never needed.
   UW_ASSERT(txn.write_set().size() > row.write_set_idx());
  return txn.write_set()[row.write_set_idx()].key();

 

  if(params.parallel_CCC){
     //We cannot keep track of correct write set idx because we will sort the write set (+ we insert table versions) => Must encode it on the spot
    std::vector<const std::string*> primary_cols;
    for(auto &idx: table_store->sql_interpreter.GetTableRegistry_const()->at(table_name).primary_col_idx){
      primary_cols.push_back(&row.column_values()[idx]);
    }
    return EncodeTableRow(table_name, primary_cols);;
  }
  else{ //can look up via write_set_idx
    UW_ASSERT(txn.write_set().size() > row.write_set_idx());
    return txn.write_set()[row.write_set_idx()].key();
  }
 
}


proto::ConcurrencyControl::Result Server::CheckReadPred(const Timestamp &txn_ts, const proto::ReadPredicate &pred, const ReadSet &txn_read_set, std::set<std::string> &dynamically_active_dependencies){
  
  //Set of potential dynamic dependencies
  std::map<std::string, std::pair<bool, std::string>> dynamic_dep_candidates; //map: key -> (active, txndig). Active if another write before reads it.
  //A dynamic dep is a read to a prepared write that is **not** in the current (active) read set, and does **not** trigger a predicate conflict
  //It is considered "active" if there is another prepared/committed write beneath this prepared write that **does** triger a conflict

  
  // std::vector<std::string> instantiated_preds;
  // for(auto &instance: pred.instantiations()){ //NOTE: there will be an iteration for each instantiation of a NestedLoop execution (right table)
  //   instantiated_preds.push_back(pred.where_clause()); //TODO: FIXME: fill in all the {} entries... Seems like this is not straightforward with fmt::format() (requires all args at once)
  // } 
 
  std::set<std::string> dynamically_active_keys; //this is local to each pred, and purely used to avoid unecessary evals.
  UW_ASSERT(!pred.pred_instances().empty());
  Debug("TX_ts: [%lu:%lu]. Pred: [%s]: %s (instance 1)", txn_ts.getTimestamp(), txn_ts.getID(), pred.table_name().c_str(), pred.pred_instances()[0].c_str());

  //Currently checking all conflict types on TableVersion 
  //TODO: Optimization  //Check TableVersion for INSERT conflicts && Check ColVersions (of the pred) for UPDATE/DELETE conflicts
  TableWriteMap::const_accessor tw;
  bool has_table = tableWrites.find(tw, pred.table_name());
  if(!has_table){
    Debug("No table writes for table [%s]", pred.table_name().c_str());
    return proto::ConcurrencyControl::COMMIT; // Panic("all tables must have a tableWrite entry.");
  } 
  auto &curr_table_writes = tw->second;


  Debug("TX_ts: [%lu:%lu]. Pred: [%s]. Check against all TX down to TS[%lu]. ", txn_ts.getTimestamp(), txn_ts.getID(), pred.table_name().c_str(), pred.table_version().timestamp() - write_monotonicity_grace);
  for(auto itr = --curr_table_writes.lower_bound(txn_ts); itr != curr_table_writes.begin(); --itr){

//for(auto itr = curr_table_writes.rbegin(); itr != curr_table_writes.rend(); ++itr){

    const Timestamp &curr_ts = itr->first;
     Debug("TX_ts: [%lu:%lu]. Check vs Write_Ts [%lu:%lu]", txn_ts.getTimestamp(), txn_ts.getID(), curr_ts.getTimestamp(), curr_ts.getID());
     //if(curr_ts > txn_ts) continue;
   
    //Skip comparing against table version itself. We've already seen it, it cannot possibly be a new write.
    if(curr_ts.getTimestamp() == pred.table_version().timestamp() && curr_ts.getID() == pred.table_version().id()) continue;

    //Bound how far we need to check by the READ Table/Col Version - grace. I.e. look at all writes s.t. read.TS >= write.TS write.TS > read.TableVersion - grace
    if(curr_ts.getTimestamp() + write_monotonicity_grace < pred.table_version().timestamp()) break;  //bound iterations until read table version

    Debug("TX_ts: [%lu:%lu]. Pred: [%s]: compare vs write TS[%lu:%lu]", txn_ts.getTimestamp(), txn_ts.getID(), pred.table_name().c_str(), itr->first.getTimestamp(), itr->first.getID());
    auto &[write_txn, commit_or_prepare] = itr->second;
    UW_ASSERT(write_txn); 
    UW_ASSERT(write_txn->has_txndigest());

    const TableWrite &txn_table_write = write_txn->table_writes().at(pred.table_name());
    //Go to this Txns TableWrite for this table
    for(auto &row: txn_table_write.rows()){
      
      const std::string &write_key = GetEncodedRow(*write_txn, row, pred.table_name());
   

      Debug("TX_ts: [%lu:%lu]. Pred: [%s]: compare vs write key [%s]", txn_ts.getTimestamp(), txn_ts.getID(), pred.table_name().c_str(), write_key.c_str());

      // /* DEBUG PRINTS FOR WRITE TXN. PRINT TABLE ROWS AND WRITE SET*/
      // int idx = 0;
      // for(auto &write: write_txn->write_set()){
      //   Debug("idx: %d. key: %s. row idx: %d", idx, write.key().c_str(), write.rowupdates().row_idx());
      //   idx++;
      // }

      // for(auto &[name, tbl_write]: write_txn->table_writes()){
      //   for(auto &row: tbl_write.rows()){
      //     Debug("[%s]. write idx: %d", name.c_str(), row.write_set_idx());
      //     int max = 3;
      //     std::cerr << "             vals: ";
      //     for(auto &val: row.column_values()){
      //       std::cerr << val << " # ";
      //       if(--max == 0) break;
      //     }
      //     std::cerr << std::endl;
      //   }
      // }
      //  Panic("stop testing here");
      // //TEST CODE FIXME: REMOVE

     
      bool force_recheck = false;
      if(dynamic_dep_candidates.count(write_key)){ //if there is already a dep candidate for this key
        if(dynamic_dep_candidates[write_key].first) continue; //if the dep is active already => do nothing, can skip

        //If the dep is currently inactive:
        if(lazy_check){
          //make active if there is a version that *could* cause conflict. "lazy" since we don't check to make sure if the version definitively makes it active or not.
          dynamic_dep_candidates[write_key].first = true; 
          continue; //Note: all dynamic deps are dynamic keys too..
        }
        else{
          //Check if there is a "discoverable" version that causes conflict (discoverable = no committed version above
          //Bypass the dynamically active key check
          force_recheck = true;
          //(AT THE END of loop): If commit, and currently candidate = false => make it so it can never happen again. (simply erase candidate => then dynamic active check below will skip)
        }
      }

      if((!force_recheck && dynamically_active_keys.count(write_key)) || 
          std::find_if(txn_read_set.begin(), txn_read_set.end(), [&write_key](const ReadMessage &read){return read.key() == write_key;}) != txn_read_set.end()){
            Debug("TX_ts: [%lu:%lu]. Can skip compare vs write key [%s]. Already in active or dynamic read set", txn_ts.getTimestamp(), txn_ts.getID(), write_key.c_str());
            continue; //skip eval this key. It's already in our read set (or dynamically active read set)
      }

      bool conflict = false;
      //for(auto &pred_instance: instantiated_preds){

      if(!row.deletion()){ //Note: Deletion by default Eval to false.
        for(auto &pred_instance: pred.pred_instances()){
         conflict = EvaluatePred(pred_instance, row, pred.table_name());
        
         if(!lazy_check && dynamic_dep_candidates.count(write_key)){
            dynamic_dep_candidates[write_key].first = true;
            continue; //No conflict because we saw prepared version that doesn't conflict. In this case MUST wait for that prepared version (active candidate)
         }
          Debug("TX_ts: [%lu:%lu]. Eval pred [%s] vs write key [%s]. Has conflict? %d", txn_ts.getTimestamp(), txn_ts.getID(), pred_instance.c_str(), write_key.c_str(), conflict);
         if(conflict) {
          return proto::ConcurrencyControl::ABSTAIN; 
         }
         //if(conflict) return (commit_or_prepare ? proto::ConcurrencyControl::ABORT : proto::ConcurrencyControl::ABSTAIN); 
           //TODO: Replace with ABORT: FIXME: To do this, need to store commit proof, and not just the TXN.
        }
      }
  
      




      //add key to dynamic read set (and dynamic dependencies)
      //Note: TODO: We have to wait for dynamic dependencies to commit. 
      //Note: We do not need to check the dynamically active keys. If there *was* a conflict, then we would've already checked against a new Tx. This is purely to minimize evaluations.
      dynamically_active_keys.insert(write_key);  
      //if(!commit_or_prepare) dynamically_active_dependencies.insert(write_txn->txndigest());
      if(!commit_or_prepare) dynamic_dep_candidates[write_key] = std::make_pair(false, write_txn->txndigest());
      else dynamic_dep_candidates.erase(write_key); //If dynamic dep is inactive, and we see a commit that doesn't violate the pred => make dep definitively inactive (erase)
    }
    //Iterate through the RowUpdates
      //check whether already in read set or bonus structure.
  }
  

          //Simpler: Put both in a map, mark a flag (prep/commit), and either upgrade prepare to commit, or remove prepares again?
        //Bound how far we need to check by the READ Table/Col Version - grace. I.e. look at all writes s.t. read.TS >= write.TS write.TS > read.TableVersion - grace


            //Pairwise TX check
                // - check whether that Tx produced a write that we should have read (i.e. fulfills read predicate). 
                //     - If yes, abort
                //         - Note: If the written row (primary key) is already part of read set (or we have a newer version for this rows primary key), don’t do anything
                //     - If no, continue (includes deletion)
                //         - If the primary key is not part of the read set already, add it to the read set (or some “bonus” structure) → this is to avoid aborting if there is older versions of the row that DO trigger the predicate
                //             - If the write was prepared only (i.e. not committed), then add a dynamic dependency to it (dynamic Active Negative Read Set). Wait for this TX to commit/abort before deciding

  for(auto &[_, candidate]: dynamic_dep_candidates){
     if(candidate.first) dynamically_active_dependencies.insert(candidate.second); //only record dep if candidate is active.
  }

  tw.release();
  return proto::ConcurrencyControl::COMMIT;
}

//TODO: Call this each time we loop throug write set! => simply go find the row via the index. (that way we don't have to loop twice.)
proto::ConcurrencyControl::Result Server::CheckTableWrites(const proto::Transaction &txn, const Timestamp &txn_ts, const std::string &table_name, const TableWrite &table_write){
  //TODO: When looping through TableWrite row => also want the idx in read set? (Currently write set has index of the belonging TableWrite row; but may want to store the opposite too.)

  //Currently always checking against TableVersion
  //TODO: optimization:  //TODO: Need to mark rows with write type?
        //If INSERT:check against Table predicates && add to TableVersion
        //If UPDATE/DELETE: check against respective col predicates && add to ColVersions. 
  TablePredicateMap::const_accessor tp;
   
  // //FIXME: REMOVE TEST CODE
  //  tablePredicates.insert(tp, table_name); 
  //  //Insert dummy TX with low TS and with high TS. No preds
  //  proto::Transaction *dummy_tx = new proto::Transaction();
  //  auto dummy_pred = dummy_tx->mutable_merged_read_set()->add_read_predicates();
  //  dummy_pred->set_table_name("flight");
  //  dummy_pred->mutable_table_version()->set_id(0);
  //  dummy_pred->mutable_table_version()->set_timestamp(0);
  //  dummy_pred->add_pred_instances("test");
  //  //TODO: insert with TS:
  //  //tp->second[]

  Debug("TX_ts: [%lu:%lu]. TableWrites: [%s]: ", txn_ts.getTimestamp(), txn_ts.getID(), table_name.c_str());

  bool has_preds = tablePredicates.find(tp, table_name);
  if(!has_preds) { 
    Debug("No preds for table [%s]. Return", table_name.c_str());
    return proto::ConcurrencyControl::COMMIT;
  }
  auto &curr_table_preds = tp->second;

  //
  // Debug("PRINT WRITE SET");
  // for(auto &write: txn.write_set()){
  //   Debug("key: %s. table_v? %d Write TS: [%lu:%lu]", write.key().c_str(), write.is_table_col_version(), txn_ts.getTimestamp(), txn_ts.getID());
  // }

  
  // For each TableWrite: Check if there are any prepared/committed Read TX with higher TS that this TableWrite could be in conflict with
  for(auto itr = curr_table_preds.upper_bound(txn_ts); itr != curr_table_preds.end(); ++itr){  //Check against all read preds (on this table) with read.TS >= write.TS
    Debug("TX_ts: [%lu:%lu]. TableWrites: [%s]. Compare vs Read with TS[%lu:%lu]", txn_ts.getTimestamp(), txn_ts.getID(), table_name.c_str(), itr->first.getTimestamp(), itr->first.getID());
    auto &[read_txn, commit_or_prepare] = itr->second;

    //TODO: Find a way to organize Predicates by Table...
    //That way we don't have to waste time looping through irrelevant preds here?
    //And multiple preds could share the same TXN loop in the CheckReadPred Function
    // FIXME: Instead of storing preds in QueryGroupMeta => store them in "table_preds" (just like TableWrites...)
        //Does QueryGroupMeta make it easier to cache though? No, could just add the Table_preds to QueryResult?
        //This seems more elegant. Even if we had multiple shards, we'd need to store the same preds for each group no?
        //CON: Read Set MGR currently is elegantly set up to handle setting preds.
              //Would need to add another interface. Would need to be threadsafe, to handle accesses from parallel Queries touching same Table...

    //Check whether the Read Txn conflicts with any of this TableWrites row
    for(auto &row: table_write.rows()){

      if(row.deletion()) continue;
      //If TX has it is read set -> cc would've handled it.
      //If it doesn't, then don't need to check -> deletion evals to false!.
     
      //check if read_txn read set already has this write key. If so, skip (we've already done normal CC on it)
          //NOTE: checking just for key presence is enough! If it is present, then Normal CC check will already have handled conflicts.
      
      const std::string &write_key = GetEncodedRow(txn, row, table_name);

      const ReadSet &txn_read_set = read_txn->has_merged_read_set() ? read_txn->read_set() : read_txn->merged_read_set().read_set();
      if(std::find_if(txn_read_set.begin(), txn_read_set.end(), [write_key](const ReadMessage &read){return read.key()==write_key;}) != txn_read_set.end()){
          Debug("TX_ts: [%lu:%lu]. Can skip compare write key [%s] vs pred. Read Txn already has it in active read set", txn_ts.getTimestamp(), txn_ts.getID(), write_key.c_str());
              continue; //skip eval this key. It's already in the predicate txns' read set 
      }

  

      UW_ASSERT(!read_txn->query_set().empty()); //if txn didn't have a query set it wouldnt be in the pred list!
      const PredSet *predSet = &read_txn->read_predicates();
      if(params.query_params.cacheReadSet || !params.query_params.mergeActiveAtClient){
          if(read_txn->has_merged_read_set()){
              predSet = &read_txn->merged_read_set().read_predicates();
          }
          //Note: It may be that this replica does not locally have the correct merged read set and hence it is empty. This is safe (see UpdateCommittedReads comments)
      }
 
      for(auto &pred: *predSet){
        if(table_name != pred.table_name()) continue;

         //Skip comparing against table version itself. The reader must've already seen it, it cannot possibly be a new write.
        if(txn_ts.getTimestamp() == pred.table_version().timestamp() && txn_ts.getID() == pred.table_version().id()) continue;

        //only check if this write is still relevant to the Reader. Note: This case should never happen, such writes should not be able to be admitted
        if(txn_ts.getTimestamp() + write_monotonicity_grace < pred.table_version().timestamp()){
          Panic("non-monotinic write should never be admitted"); 
          //NOTE: Not quite true locally. This replica might not have seen a TableVersion high enough to cause this TX to be rejected; meanwhile, the read might have read the TableVersion elsewhere
          //-- but as a whole, a quorum of replicas should be rejecting this tx.
          continue;
        } 

        //For each pred, insantiate all, and evaluate.
        // for(auto &instance: pred.instantiations()){
        //   std::string pred_instance = pred.where_clause(); //TODO: FIXME: fill in all the {} entries... Seems like this is not straightforward with fmt::format() (requires all args at once)
        for(auto &pred_instance: pred.pred_instances()){
          Debug("TX_ts: [%lu:%lu]. Eval write key [%s] vs read pred [%s].", txn_ts.getTimestamp(), txn_ts.getID(), write_key.c_str(), pred_instance.c_str());
          bool conflict = EvaluatePred(pred_instance, row, pred.table_name());
          Debug("TX_ts: [%lu:%lu]. Eval write key [%s] vs read pred [%s]. Has conflict? %d", txn_ts.getTimestamp(), txn_ts.getID(), write_key.c_str(), pred_instance.c_str(), conflict);
          if(conflict) return proto::ConcurrencyControl::ABSTAIN; 
          //if(conflict) return commit_or_prepare ? proto::ConcurrencyControl::ABSTAIN: proto::ConcurrencyControl::ABSTAIN; 
                                //TODO: Replace with ABORT: FIXME: To do this, need to access a commit proof, and not just the TXN.
          
        } 
      }
      
      //OLD VERSION: Check each query of the read txn.
      // for(auto &query_md : read_txn->query_set()){
      //   UW_ASSERT(query_md.group_meta().count(groupIdx));
      //   const proto::QueryGroupMeta &group_meta = query_md.group_meta().at(groupIdx); //only need to look at the pred for partitions this group is responsible for (Note: we don't support partitions currently)

      //   const proto::ReadSet &read_set = group_meta.query_read_set();
      //   //TODO: If cached: need to get read predicates on demand... 
      //   for(auto &pred: read_set.read_predicates()){
      //     if(table_name != pred.table_name()) continue;
      //     //only check if this write is still relevant to the Reader. Note: This case should never happen, such writes should not be able to be admitted
      //     if(txn_ts.getTimestamp() + write_monotonicity_grace < pred.table_version().timestamp()){
      //       Panic("write should never be admitted"); 
      //       //NOTE: Not quite true locally. This replica might not have seen a TableVersion high enough to cause this TX to be rejected; meanwhile, the read might have read the TableVersion elsewhere
      //       //-- but as a whole, a quorum of replicas should be rejecting this tx.
      //       continue;
      //     } 

      //     //For each pred, insantiate all, and evaluate.
      //     for(auto &instance: pred.instantiations()){
      //       std::string pred_instance = pred.where_clause(); //TODO: FIXME: fill in all the {} entries... Seems like this is not straightforward with fmt::format() (requires all args at once)
            
      //       bool conflict = EvaluatePred(pred_instance, row);
      //       if(conflict) return proto::ConcurrencyControl::ABSTAIN; 
      //       //if(conflict) return commit_or_prepare ? proto::ConcurrencyControl::ABSTAIN: proto::ConcurrencyControl::ABSTAIN; 
      //                             //TODO: Replace with ABORT: FIXME: To do this, need to access a commit proof, and not just the TXN.
            
      //     } 
      //   }
      // }
    }
  }
  // - for each written table/col version:
  //         - Note: For an insert, this TX must be writing a new table version. For an update, it must be writing certain Col Versions:
  //     - Check all stored read preds P such that: P.readTS - grace < write.TS and write.TS < P.origin.TS. For each such pred:
  //         - Check whether (row.primaryKey, write.TS) is in pred.originTX.read-set (i.e. whether already active)
  //         - If not: Check whether P(row) = true. If so, abort.

  //Optimize: save dynamically active read set to avoid more evals? but then would need to store version also, to compare whether this current write is relevant or not.
  return proto::ConcurrencyControl::COMMIT;
}

//Note: We are assuming TS are unique to a TX here. Duplicates should already have been filtered out (it should not be possible that 2 TX with same TS commit)
void Server::RecordReadPredicatesAndWrites(const proto::Transaction &txn, const Timestamp &ts, bool commit_or_prepare){
  
  Debug("RECORDING READ PRED AND WRITE");
 
  //NOTE: 
    //Throughout the CC process we are holding a lock on a given table name
    //This ensure that no new Txn can be admitted without seeing these reads/writes.
    //Recording new TableWrites will influence the Monotonicity check for writes, thus guaranteeing that no older writes can be admitted.
    
  
  //TODO: When adding a new pred: Garbage collect preds older than some time (past grace)
  //At that point, do not allow any reads with TableVersions that are below GC watermark
  //TODO: Sanity check GC correctness. Currently just placeholder code.
  int gc_delta = 10000; //ms
  Timestamp lowWatermark(timeServer.GetTime() - gc_delta);


  //Record all ReadPredicates   //Store a reference: table_name -> txn
  const PredSet *predSet = &txn.read_predicates(); //If we are not caching, and clientMerges => then all preds must be in read_predicates(). Otherwise, we must've merged
  if(!txn.query_set().empty() && (params.query_params.cacheReadSet || !params.query_params.mergeActiveAtClient)){
   
    if(txn.has_merged_read_set()){
       predSet = &txn.merged_read_set().read_predicates();
    }
    else{
      // Note: if txn doesn't have merged read set, even though it seems like it should, then it is because it locally cached a different read set
      // In this case, we can just skip locally applying the read predicates/read set (it's safe: sufficiently many other replicas WILL do it correctly)
      // I.e.: If the TX got enough commit votes (3f+1), then at least 2f+1 correct replicas must have had the correct readSet. Those suffice for safety
      // conflicts. this replica will STILL apply the TableWrites, so visibility isn't impeded.
    }
   
  }
  Debug("Prepare: predSet has [%d] read preds", predSet->size());
 

  for(auto &pred: *predSet){
    std::cerr << "Record pred: " << pred.pred_instances()[0] << std::endl;
    TablePredicateMap::accessor tp;
    tablePredicates.insert(tp, pred.table_name());
    auto &preds = tp->second;
    preds[ts] = std::make_pair(&txn, commit_or_prepare); //Add new pred (or overwrite existing one)
      //TODO: Erase everything below the GC watermark. 
      // for (auto it = m.cbegin(); it != m.cend(); ){ // no "++"!
      //   if (it->first < lowWatermark) m.erase(it++);
      //   else break;
      // }
    tp.release();
    Debug("Insert ReadPred for table: %s. Read TS [%lu:%lu]", pred.table_name().c_str(), ts.getTimestamp(), ts.getID());
  }
  
   // for(auto &query_md : txn.query_set()){
  //   const proto::QueryGroupMeta &group_meta = query_md.group_meta().at(groupIdx); //only need to look at the pred for partitions this group is responsible for (Note: we don't support partitions currently)

  //   const proto::ReadSet &read_set = group_meta.query_read_set();
  //   //TODO: If cached: get read predicates.
  //   for(auto &pred: read_set.read_predicates()){
  //        TablePredicateMap::accessor tp;
  //       tablePredicates.insert(tp, pred.table_name());
  //       auto &preds = tp->second;
  //       preds[ts] = std::make_pair(&txn, commit_or_prepare); //Add new pred (or overwrite existing one)
  //         //TODO: Erase everything below the GC watermark. 
  //         // for (auto it = m.cbegin(); it != m.cend(); ){ // no "++"!
  //         //   if (it->first < lowWatermark) m.erase(it++);
  //         //   else break;
  //         // }
  //       tp.release();
  //   }
  // }
 

  //Record all TableWrites  //Store a reference: table_name -> txn
  for(auto &[table_name, _]: txn.table_writes()){
     std::cerr << "Record Table Write: " << table_name << std::endl;
    TableWriteMap::accessor tw;
    tableWrites.insert(tw, table_name);
    auto &write = tw->second;
    write[ts] = std::make_pair(&txn, commit_or_prepare); //Add new write (or overwrite existing one)
    //NOTE: Can't simply GC writes below the TS we are admitting. A pred that comes in might have seen an old table version, thus the writes need to be present
            //We would have to abort TX that have preds with too low TableVersion (below lowWatermark) => then we can GC writes.
    
    // for (auto it = m.cbegin(); it != m.cend(); ){ // no "++"!
    //   if (it->first < lowWatermark) m.erase(it++);
    //   else break;
    // }
    tw.release();
    Debug("Insert TableWrite for table: %s. Write TS [%lu:%lu]", table_name.c_str(), ts.getTimestamp(), ts.getID());
  }
}


void Server::ClearPredicateAndWrites(const proto::Transaction &txn){
  Timestamp ts(txn.timestamp()); 

  //Clear all ReadPredicates   
  const PredSet *predSet = &txn.read_predicates();
  if(!txn.query_set().empty() && (params.query_params.cacheReadSet || !params.query_params.mergeActiveAtClient)){
    if(txn.has_merged_read_set()){
      predSet = &txn.merged_read_set().read_predicates();
    }
  }
 
  for(auto &pred: *predSet){
    TablePredicateMap::accessor tp;
    if(tablePredicates.find(tp, pred.table_name())){
      auto &preds = tp->second;
      preds.erase(ts);
    }
    tp.release();
  }

      // for(auto &query_md : txn.query_set()){
      //   const proto::QueryGroupMeta &group_meta = query_md.group_meta().at(groupIdx); //only need to look at the pred for partitions this group is responsible for (Note: we don't support partitions currently)

      //   const proto::ReadSet &read_set = group_meta.query_read_set();
      //   //TODO: If cached: get read predicates.
      //   for(auto &pred: read_set.read_predicates()){
      //       TablePredicateMap::accessor tp;
      //       tablePredicates.find(tp, pred.table_name());
      //       auto &preds = tp->second;
      //       preds.erase(ts);
      //       tp.release();
      //   }
      // }
   
 
  //Clear all TableWrites
  for(auto &[table_name, _]: txn.table_writes()){
    TableWriteMap::accessor tw;
    if(tableWrites.find(tw, table_name)){
      auto &writes = tw->second;
      writes.erase(ts);
    }
    tw.release();
  }
}

//NOTE: if we store preds inside TX, then we need to GC all record of TXN if we intend to clear up space...

//TODO: Garbage collect 
    // committed Reads ==> Don't allow new reads below GC watermark. GC all below watermark.
    // committed Writes ==> Don't allow new writes below GC watermark. GC all but the last value < GC watermark. (so that any read > watermark still has a write to read)
          //TODO: Would need to GC inside Peloton too.
    // prepared read/writes (transient anyways, not high priority)
    // prepared/committed readPredicates ==> Don't allow new preds below GC watermark. GC all read preds below GC.
bool Server::CheckGCWatermark(const Timestamp &ts) {
  int gc_delta = 10000; //ms
  Timestamp lowWatermark(timeServer.GetTime() - gc_delta);
  Debug("GC low watermark: %lu.", lowWatermark.getTimestamp());
  if(ts < lowWatermark){
   Panic("ts: %lx below GC threshold. lowWatermark: %lx", ts.getTimestamp(), lowWatermark.getTimestamp());
   return false;
  }
  return true;
}

bool Server::EvaluatePred(const std::string &pred, const RowUpdates &row, const std::string &table_name){
  return table_store->sql_interpreter.EvalPred(pred, table_name, row);
}


//PELOTON BASED EVALUATOR
 bool Server::EvaluatePred_peloton(const std::string &pred, const RowUpdates &row, const std::string &table_name){

  //return false; //FIXME: REMOVE
  std::cerr << "Inside EvalPred in semantic CC" << std::endl;
  std::cerr << "The predicate evalpred is " << pred << std::endl;
  std::string full_predicate = "SELECT * FROM " + table_name + " WHERE " + pred;
  //TODO: FILL IN
  // Call the PostgresParser
  auto parser = peloton::parser::PostgresParser::GetInstance();
  std::unique_ptr<peloton::parser::SQLStatementList> stmt_list(parser.BuildParseTree(full_predicate).release());
  if (!stmt_list->is_valid) {
    std::cerr << "Parsing failed" << std::endl;
  }

  auto sql_stmt = stmt_list->GetStatement(0);

  // Only process select statements
  if (sql_stmt->GetType() != peloton::StatementType::SELECT) return false;

  auto select_stmt = (peloton::parser::SelectStatement *)sql_stmt;

  auto where_clause = select_stmt->where_clause->Copy();
  std::cerr << "The parsed where clause evalpred is " << where_clause->GetInfo() << std::endl;
  std::shared_ptr<peloton::expression::AbstractExpression> sptr(where_clause);
  peloton::optimizer::PlanGenerator plan_generator;
  
  ColRegistry *col_registry = table_store->sql_interpreter.GetColRegistry(table_name);
  std::cerr << "Before generating predicate from col registry" << std::endl;
  auto predicate = plan_generator.GeneratePredicateForScanColRegistry(sptr, "", col_registry);

  std::cerr << "The parsed predicate evalpred is " << predicate->GetInfo() << std::endl;
  peloton::catalog::Schema *schema = ConvertColRegistryToSchema(col_registry);
  std::cerr << "Past convert col registry to schema" << std::endl;
  
  auto result = Eval(predicate.get(), row, schema);
  std::cerr << "Result from eval pred is " << result << std::endl;
  return result;
 }

 bool Server::Eval(peloton::expression::AbstractExpression *predicate, const RowUpdates row, peloton::catalog::Schema *schema) { 
  std::unique_ptr<peloton::storage::Tuple> tuple(new peloton::storage::Tuple(schema, true));

  // TODO: Make comprehensive with all types
  for(int i = 0; i < row.column_values().size(); ++i){
    if (schema->GetColumn(i).GetType() == peloton::type::TypeId::INTEGER) {
      int32_t val = std::stoi(row.column_values()[i]);
      tuple->SetValue(i, peloton::type::Value(peloton::type::TypeId::INTEGER, val));
    } else if (schema->GetColumn(i).GetType() == peloton::type::TypeId::VARCHAR) {
      tuple->SetValue(i, peloton::type::Value(peloton::type::TypeId::VARCHAR, row.column_values()[i]));
    } else if (schema->GetColumn(i).GetType() == peloton::type::TypeId::DECIMAL) {
      double val = atof(row.column_values()[i].c_str());
      tuple->SetValue(i, peloton::type::Value(peloton::type::TypeId::DECIMAL, val));
    } else if (schema->GetColumn(i).GetType() == peloton::type::TypeId::BIGINT) {
      int64_t val = std::atoll(row.column_values()[i].c_str());
      tuple->SetValue(i, peloton::type::Value(peloton::type::TypeId::BIGINT, val));
    }   
  }

  std::cerr << "Tuple values are" << std::endl;

  for (int i = 0; i < row.column_values().size(); ++i) {
    std::cerr << "Tuple col " << schema->GetColumn(i).GetName() << " is " << tuple.get()->GetValue(i).ToString() << std::endl;
  }

  //std::cerr << "Tuple is " << tuple.get()->GetInfo() << std::endl;
  auto result = predicate->Evaluate(tuple.get(), nullptr, nullptr);
  return result.IsTrue();
}

peloton::catalog::Schema* Server::ConvertColRegistryToSchema(ColRegistry *col_registry) {
  std::cerr << "trying to parse" << std::endl;
  std::vector<peloton::catalog::Column> columns;
  for (int i = 0; i < col_registry->col_names.size(); i++) {
    auto name = col_registry->col_names[i];
    auto type = col_registry->col_name_type.at(name);

    if (type == "FLOAT") type = "DECIMAL";
    if (type == "TEXT") type = "VARCHAR";
    std::cerr << "TYPE: " << type << std::endl;
    auto type_id = peloton::StringToTypeId(type);
    auto column = peloton::catalog::Column(type_id, peloton::type::Type::GetTypeSize(type_id), name, true);
    columns.push_back(column);
  }

  std::cerr << "finished id col" << std::endl;
  peloton::catalog::Schema *schema = new peloton::catalog::Schema(columns);
  return schema;
}


}