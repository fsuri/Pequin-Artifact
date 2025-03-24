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

#include "store/sintrstore/common2.h"

namespace sintrstore {
  
bool ValidateTransactionTableWrite(const proto::CommittedProof &proof, const std::string *txnDigest, const Timestamp &timestamp, 
    const std::string &key, const std::string &value, const std::string &table_name, sql::QueryResultProtoWrapper *query_result,
    SQLTransformer *sql_interpreter,
    const transport::Configuration *config, bool signedMessages,
    KeyManager *keyManager, Verifier *verifier)
{


  // Debug("[group %i] Trying to validate committed TableWrite.", group);
  
  //*query_result = std::move(sql::QueryResultProtoWrapper(value));
  SQLResultProto proto_result;

  if(value.empty()){
    //If the result is empty, then the TX must have written a row version that is either a) a delete, or b) does not fulfill the predicate
    //iterate through write set, and check that key is found, 
    for (const auto &write : proof.txn().write_set()) {
      if (write.key() == key) {
        if(!write.has_rowupdates() || !write.rowupdates().has_row_idx()) return false; // there is no associated TableWrite
        
        if(write.rowupdates().deletion()) return true;

        //TODO: Else: check if the row indeed does not fulfill the predicate.
        //For now just accepting it.
        return true;
        //return false;
      }
    }
  }

  else {
    //there is a result. Try to see if it's valid.
    proto_result.ParseFromString(value);
  }
  query_result->SetResult(proto_result);
  
  //query_result = new sql::QueryResultProtoWrapper(value); //query_result takes ownership
  //turn value into Object //TODO: Can we avoid the redundant de-serialization in client.cc? ==> Modify prcb callback to take QueryResult as arg. 
                              //Then need to change that gcb = prcb (no longer true)

  //NOTE: Currently useless line of code: If empty ==> no Write ==> We would never even enter Validate Transaction branch  //FIXME: False statement. We can enter if res exists but is null
          //We don't send empty results, we just send nothing.
          //if we did send result: if query_result empty => return true. No proof needed, since replica is reporting that no value for the requested read exists (at the TS)
  if(query_result->empty()){
    return true;
  } 


  if (proof.txn().client_id() == 0UL && proof.txn().client_seq_num() == 0UL) {
    // TODO: this is unsafe, but a hack so that we can bootstrap a benchmark
    //    without needing to write all existing data with transactions
    Debug("Accept genesis proof");
    return true; //query_result->empty(); //Confirm that result is empty. (Result must be empty..)
  }

  UW_ASSERT(query_result->size() == 1); //Point read should have just one row.

  //Check that txn in proof matches reported timestamp
  if (Timestamp(proof.txn().timestamp()) != timestamp) {
    Debug("VALIDATE timestamp failed for txn %lu.%lu: txn ts %lu.%lu != returned ts %lu.%lu.", proof.txn().client_id(), proof.txn().client_seq_num(),
      proof.txn().timestamp().timestamp(), proof.txn().timestamp().id(), timestamp.getTimestamp(), timestamp.getID());
    return false;
  }

  //Check that Commit Proof is correct
  if (signedMessages && !ValidateCommittedProof(proof, txnDigest, keyManager, config, verifier)) {
    Debug("VALIDATE CommittedProof failed for txn %lu.%lu.", proof.txn().client_id(), proof.txn().client_seq_num());
    Panic("Verification should be working");
    return false;
  }

  int32_t row_idx;
  //Check that write set of proof contains key.
  bool keyInWriteSet = false;
  for (const auto &write : proof.txn().write_set()) {
    if (write.key() == key) {
      keyInWriteSet = true;

      if(!write.has_rowupdates() || !write.rowupdates().has_row_idx()) return false;
      row_idx = write.rowupdates().row_idx();
      break;
    }
  }
  
  if (!keyInWriteSet) {
    Debug("VALIDATE value failed for txn %lu.%lu; key %s not written.", proof.txn().client_id(), proof.txn().client_seq_num(), BytesToHex(key, 16).c_str());
    return false;
  }

  //Then check that row idx of TableWrite wrote a row whose column values == result.column_values (and is not a deletion)
          //Note: check result column name --> find matching column name in TableWrite and compare value
             // ==> For Select * or Select subset of columns statements this is sufficient
          //If column name is some "creation" (e.g. new col name, or some operation like Count, Max) then ignore --> this is too complex to prototype


  //TODO: For real system need to replay Query statement on the TableWrite row. For our prototype we just approximate it.

  // size_t pos = key.find(unique_delimiter); 
  // UW_ASSERT(pos != std::string::npos);
  // std::string table_name = key.substr(0, pos); //Extract from Key
  const TableWrite &table_write = proof.txn().table_writes().at(table_name); //FIXME: Throw exception if not existent. /-->change to find
  const RowUpdates &row_update = table_write.rows()[row_idx];

  ColRegistry *col_registry = sql_interpreter->GetColRegistry(table_name); 
  int col_idx = 0;
  for(int i = 0; i < query_result->num_columns(); ++i){
    //find index of column name  -- if not present in table write --> return false
    const std::string &col_name = query_result->name(i);
    
    //then find right col value and compare
    col_idx = col_registry->col_name_index[col_name]; 
      //while(col_name != table_write.column_names) If storing column names in table write --> iterate through them to find matching col (idx).  Assuming here column names are in the same order.

  }
  Debug("VALIDATE TableWrite value successfully for txn %lu.%lu key %s", proof.txn().client_id(), proof.txn().client_seq_num(), key.c_str());
  return true;
}

void AddWriteSetIdx(proto::Transaction &txn){

  //Correct the write_set_idx according to the position of the write_key *after* sorting.
  const std::string *curr_table;
  for(int i=0; i<txn.write_set_size();++i){
    auto &write = txn.write_set()[i];
    if(write.is_table_col_version()){
      //curr_table = &write.key(); //Note: This works because we've inserted write keys for all of our Tablewrites, and the write keys are sorted.
      //Use DecodeTable if we are using TableEncoding. If not, use line above.
      curr_table = DecodeTable(write.key()); //Note: This works because we've inserted write keys for all of our Tablewrites.
      Debug("Print: Curr Table: %s", curr_table->c_str());
      UW_ASSERT(txn.table_writes().count(*curr_table));
    }
    else{
      (*txn.mutable_table_writes()->at(*curr_table).mutable_rows())[write.rowupdates().row_idx()].set_write_set_idx(i);
    }
  }
}

} // namespace sintrstore
