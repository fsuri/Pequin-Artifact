/***********************************************************************
 *
 * store/sintrstore/querysync-servertests.cc: 
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *            
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 **********************************************************************/


#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>


#include "store/sintrstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include <sstream>
#include <list>
#include <utility>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/sintrstore/common.h"
#include "store/sintrstore/phase1validator.h"
#include "store/sintrstore/localbatchsigner.h"
#include "store/sintrstore/sharedbatchsigner.h"
#include "store/sintrstore/basicverifier.h"
#include "store/sintrstore/localbatchverifier.h"
#include "store/sintrstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

#include "store/common/query_result/query_result_proto_builder.h"
#include "server.h"

namespace sintrstore {

std::string Server::TEST_QUERY_f(uint64_t q_seq_no)
{
    std::string test_result_string = std::to_string(q_seq_no);
    std::vector<std::string> result_row;
    result_row.push_back(test_result_string);
    result_row.push_back(std::to_string(42));
    sql::QueryResultProtoBuilder queryResultBuilder;
    queryResultBuilder.add_column("key");
    queryResultBuilder.add_column("value");
    queryResultBuilder.add_row(result_row.begin(), result_row.end());

    std::string dummy_result = queryResultBuilder.get_result()->SerializeAsString();
    return dummy_result;
}

void Server::TEST_QUERY_f(std::string &result)
{
    std::string toy_txn("toy_txn");
    Timestamp toy_ts(0, 2); //set to genesis time.
    sql::QueryResultProtoBuilder queryResultBuilder;
    queryResultBuilder.add_columns({"key", "value"});
    std::vector<std::string> result_row = {"0", "42"};
    queryResultBuilder.add_row(result_row.begin(), result_row.end());
    result = queryResultBuilder.get_result()->SerializeAsString();
}



void Server::TEST_QUERY_f(proto::Write *write, proto::PointQueryResultReply *pointQueryReply){
    ///////////
        //Toy Transaction
        std::string toy_txn("toy_txn");
        Timestamp toy_ts(0, 2); //set to genesis time.
        sql::QueryResultProtoBuilder queryResultBuilder;
        queryResultBuilder.add_columns({"key", "value"});
        std::vector<std::string> result_row = {"0", "42"};
        queryResultBuilder.add_row(result_row.begin(), result_row.end());
        std::string toy_result = queryResultBuilder.get_result()->SerializeAsString();

        //Panic("stop here");
        // //Create Toy prepared Tx
        if(id ==0){
        
            write->set_prepared_value(toy_result);
            std::cerr << "SENT PREPARED RESULT: " << write->prepared_value() << std::endl;
            write->set_prepared_txn_digest(toy_txn);
            toy_ts.serialize(write->mutable_prepared_timestamp());
        }
    

        //Create Toy committed Tx with genesis proof
        // proto::CommittedProof *genesis_proof = new proto::CommittedProof();
        // genesis_proof->mutable_txn()->set_client_id(0);
        // genesis_proof->mutable_txn()->set_client_seq_num(0);
        // toy_ts.serialize(genesis_proof->mutable_txn()->mutable_timestamp());

        // committed.insert(std::make_pair(toy_txn, genesis_proof));  //TODO: Need to move this elsewhere so all servers have it.

        // write->set_committed_value(toy_result);
        // toy_ts.serialize(write->mutable_committed_timestamp());
        // *pointQueryReply->mutable_proof() = *genesis_proof;


        //Create Toy committed Tx with real proof tx -- create toy "real" QC

        sql::QueryResultProtoBuilder queryResultBuilder2;
        queryResultBuilder2.add_columns({"key", "val"});
        result_row = {"0", "41"};
        queryResultBuilder2.add_row(result_row.begin(), result_row.end());
        std::string toy_result2 = queryResultBuilder2.get_result()->SerializeAsString();

        Timestamp toy_ts_c(0, 1);

        proto::CommittedProof *real_proof = new proto::CommittedProof();
        proto::Transaction *txn = real_proof->mutable_txn();
        real_proof->mutable_txn()->set_client_id(0);
        real_proof->mutable_txn()->set_client_seq_num(1);
        toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
        TableWrite &table_write = (*real_proof->mutable_txn()->mutable_table_writes())["datastore"];
        RowUpdates *row = table_write.add_rows();
        row->add_column_values("0");
        row->add_column_values("41");
        WriteMessage *write_msg = real_proof->mutable_txn()->add_write_set();
        write_msg->set_key("datastore#alice");
        write_msg->mutable_rowupdates()->set_row_idx(0);

        //Add relal qC:
        // proto::Signatures &sigs = (*real_proof->mutable_p1_sigs())[id];
        // sigs.add_sigs();
        // SignMessage()

        //committed[toy_txn]  = real_proof;  //TODO: Need to move this elsewhere so all servers have it.

        write->set_committed_value(toy_result2);
        
        toy_ts_c.serialize(write->mutable_committed_timestamp());
        *pointQueryReply->mutable_proof() = *real_proof;
}

void Server::TEST_READ_SET_f(sintrstore::proto::QueryResult *result)
{
      Debug("BEGIN READ SET:"); // just for testing
              
                for(auto &read : result->query_read_set().read_set()){
                //for(auto &[key, ts] : read_set){
                  //std::cerr << "key: " << key << std::endl;
                  Debug("Cached Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
                  //Debug("[group %d] Read key %s with version [%lu:%lu]", group, key.c_str(), ts.timestamp(), ts.id());
                }
              
       Debug("END READ SET.");
}

void Server::TEST_SNAPSHOT_f(proto::Query *query, QueryMetaData *query_md)
{

    std::string test_txn_id = "[test_id_of_length_32 bytes----]";
    proto::CommittedProof *proof = new proto::CommittedProof();
    //Genesis proof: client id + client seq = 0.
        proof->mutable_txn()->set_client_id(0);
    proof->mutable_txn()->set_client_seq_num(0);
    uint64_t ts = 5UL << 32; //timeServer.GetTime(); 
    proof->mutable_txn()->mutable_timestamp()->set_timestamp(ts);
    proof->mutable_txn()->mutable_timestamp()->set_id(0UL);
    proof->mutable_txn()->add_involved_groups(0);
    committed[test_txn_id] = proof;

    if(!TEST_MATERIALIZE){
        materializedMap::accessor mat;
        materialized.insert(mat, test_txn_id);
        mat.release();
    }
    
    if(!TEST_MATERIALIZE_TS){
            ts_to_tx.insert(std::make_pair(MergeTimestampId(ts, 0UL), test_txn_id));
    }
    
    
    
    proto::Phase1 p1 = proto::Phase1();
    p1.set_req_id(1);

    std::vector<::google::protobuf::Message *> p1s;
    p1s.push_back(proof->mutable_txn());
    std::vector<proto::SignedMessage *> sp1s;
    sp1s.push_back(p1.mutable_signed_txn());
    SignMessages(p1s, keyManager->GetPrivateKey(keyManager->GetClientKeyId(0)), 0, sp1s, params.merkleBranchFactor);

    p1MetaDataMap::accessor c;
    p1MetaData.insert(c, test_txn_id);
    c->second.hasSignedP1 = true;
    c->second.signed_txn = p1.release_signed_txn();
    c.release();


    std::string bonus_test = "BONUS TEST";
    proof = new proto::CommittedProof();
    //Genesis proof: client id + client seq = 0.
    proof->mutable_txn()->set_client_id(0);
    proof->mutable_txn()->set_client_seq_num(0);
    proof->mutable_txn()->mutable_timestamp()->set_timestamp(ts);
    proof->mutable_txn()->mutable_timestamp()->set_id(1UL);
    proof->mutable_txn()->add_involved_groups(0);
    committed[bonus_test] = proof;

    if(!TEST_MATERIALIZE){
        materializedMap::accessor mat;
        materialized.insert(mat, bonus_test);
        mat.release();
    }
    
    if(!TEST_MATERIALIZE_TS){
            ts_to_tx.insert(std::make_pair(MergeTimestampId(ts, 1UL), bonus_test));
    }
    
    // TOY INSERT TESTING.
    //-- real tx-ids are cryptographic hashes of length 256bit = 32 byte.
    for(auto const&[tx_id, proof] : committed){
        if(tx_id == "" || tx_id == "toy_txn") continue;
        const proto::Transaction *txn = &proof->txn();
        query_md->snapshot_mgr.AddToLocalSnapshot(tx_id, txn, true);

        Debug("Proposing committed txn_id [%s] for local Query Sync State[%lu:%lu:%d]", BytesToHex(tx_id, 16).c_str(), query->query_seq_num(), query->client_id(), query->retry_version());
        
        //Adding some dummy tx to prepared.
        preparedMap::accessor p;
        prepared.insert(p, tx_id);
        Timestamp ts(txn->timestamp());
        p->second = std::make_pair(ts, txn);
        p.release();
    }
    //Not threadsafe, but just for testing purposes.
    for(preparedMap::const_iterator i=prepared.begin(); i!=prepared.end(); ++i ) {
        const std::string &tx_id = i->first;
        const proto::Transaction *txn = i->second.second;
        query_md->snapshot_mgr.AddToLocalSnapshot(tx_id, txn, false);
        Debug("Proposing prepared txn_id [%s] for local Query Sync State[%lu:%lu:%d]", BytesToHex(tx_id, 16).c_str(), query->query_seq_num(), query->client_id(), query->retry_version());
    }

}



void Server::TEST_MATERIALIZE_f(){
    //materialize the missing Tx in order to wake.
    std::string test_txn_id = "[test_id_of_length_32 bytes----]";
    std::string bonus_test = "BONUS TEST";
    uint64_t ts = 5UL << 32;
    
    
    
    if(TEST_MATERIALIZE_TS){
        ts_to_tx.insert(std::make_pair(MergeTimestampId(ts, 0UL), test_txn_id));
        ts_to_tx.insert(std::make_pair(MergeTimestampId(ts, 1UL), bonus_test));
    }

    
    CheckWaitingQueries(test_txn_id, ts, 0); //this should wake TS but make TX wait on mat
    
    materializedMap::accessor mat;
    std::cerr << "MATERIALIZE MISSING TRANSACTION " << BytesToHex(bonus_test, 16) << std::endl;
    materialized.insert(mat, bonus_test);
    CheckWaitingQueries(bonus_test, ts, 1); //this will wake on this TS, but won't be able to start callback because there is a waiting TX
    mat.release();

    if(!TEST_MATERIALIZE_FORCE){
            sleep(1);        
        std::cerr << "MATERIALIZE MISSING TRANSACTION " << BytesToHex(test_txn_id, 16) << std::endl;
        materialized.insert(mat, test_txn_id); //this will wake TX
        CheckWaitingQueries(test_txn_id, ts, 0); //this should go wait on TX mat
        mat.release();
    }
    if(TEST_MATERIALIZE_FORCE){
        // sleep(1);
        // proto::Transaction *txn = new proto::Transaction();
        // //Genesis proof: client id + client seq = 0.
        // txn->set_client_id(0);
        // txn->set_client_seq_num(0);
        // txn->mutable_timestamp()->set_timestamp(ts);
        // txn->mutable_timestamp()->set_id(0UL);
        // txn->add_involved_groups(0);

        // const proto::CommittedProof* committedProof = nullptr; //abort conflict
        // const proto::Transaction *abstain_conflict = nullptr;
        // sockaddr_in addr;
        // const TCPTransportAddress remote = TCPTransportAddress(addr);

        // //Note: if forceMaterialize is set to false, this should still forceMat because we registered
        // HandlePhase1CB(0, proto::ConcurrencyControl_Result_ABORT, committedProof, test_txn_id, txn, remote, abstain_conflict, true, false);
    }
}

void Server::TEST_MATERIALIZE_FORCE_f(const proto::Transaction *txn, const std::string &tx_id){
    //In this case: RegisterForceMaterialization should trigger instant ForceMaterialize. HandleP1CB call should not re-issue ForceMat.
    proto::Transaction *tx = new proto::Transaction();
    //Genesis proof: client id + client seq = 0.
    tx->set_client_id(0);
    tx->set_client_seq_num(0);
        uint64_t ts = 5UL << 32;
    tx->mutable_timestamp()->set_timestamp(ts);
    tx->mutable_timestamp()->set_id(0UL);
    tx->add_involved_groups(0);
    txn = tx;

    const proto::CommittedProof* committedProof = nullptr; //abort conflict
    const proto::Transaction *abstain_conflict = nullptr;
    sockaddr_in addr;
    TCPTransportAddress remote =TCPTransportAddress(addr);
    const TransportAddress *remote_orig = &remote;
    proto::ConcurrencyControl::Result res = proto::ConcurrencyControl_Result_ABSTAIN;
    uint64_t req_id = 0;
    bool wake = false;
    bool force = false;
    
    BufferP1Result(res, committedProof, tx_id, req_id, remote_orig, wake, force, true, 0);
}

void Server::TEST_READ_MATERIALIZED_f() {
    TEST_READ_MATERIALIZED = false; //only do once
    std::string test_txn_id = "dont read this tx2";
    proto::CommittedProof *proof = new proto::CommittedProof();
    proof->mutable_txn()->set_client_id(1);
    proof->mutable_txn()->set_client_seq_num(0);
    uint64_t ts = 5UL << 32; //timeServer.GetTime(); 
    proof->mutable_txn()->mutable_timestamp()->set_timestamp(ts);
    proof->mutable_txn()->mutable_timestamp()->set_id(1UL); //just above the genesis TXs
    proof->mutable_txn()->add_involved_groups(0);
    committed[test_txn_id] = proof;
    ts_to_tx.insert(std::make_pair(MergeTimestampId(ts, 1UL), test_txn_id));

    std::string table_name = "table_5";
    TableWrite &table_write = (*proof->mutable_txn()->mutable_table_writes())[table_name];
    table_write.add_column_names("key");
    table_write.add_column_names("value");
    RowUpdates *new_row = table_write.add_rows();
    new_row->add_column_values("85");
    new_row->add_column_values("100");

    Timestamp t(ts, 1UL);
    ApplyTableWrites(proof->txn(), t, test_txn_id, proof, false, true); //prepared, force materialize
}

void Server::TEST_READ_FROM_SS_f(){
    TEST_READ_FROM_SS = false; //only do once
    std::string test_txn_id = "dont read this tx";
    proto::CommittedProof *proof = new proto::CommittedProof();
    proof->mutable_txn()->set_client_id(1);
    proof->mutable_txn()->set_client_seq_num(0);
    uint64_t ts = 5UL << 32; //timeServer.GetTime(); 
    proof->mutable_txn()->mutable_timestamp()->set_timestamp(ts);
    proof->mutable_txn()->mutable_timestamp()->set_id(1UL); //just above the genesis TXs
    proof->mutable_txn()->add_involved_groups(0);
    committed[test_txn_id] = proof;
    ts_to_tx.insert(std::make_pair(MergeTimestampId(ts, 1UL), test_txn_id));

    std::string table_name = "table_5";
    TableWrite &table_write = (*proof->mutable_txn()->mutable_table_writes())[table_name];
    table_write.add_column_names("key");
    table_write.add_column_names("value");
    RowUpdates *new_row = table_write.add_rows();
    new_row->add_column_values("84");
    new_row->add_column_values("100");

    Timestamp t(ts, 1UL);
    //Note: If write committed = SSread should still see it; if prepared, it should not.
    bool commit_or_prepare = false; //true
    ApplyTableWrites(proof->txn(), t, test_txn_id, proof, commit_or_prepare, false);
}




} // namespace sintrstore

