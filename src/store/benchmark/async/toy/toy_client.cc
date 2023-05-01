/***********************************************************************
 *
 * Copyright 2022 Florian Suri-Payer <fsp@cs.cornell.edu>
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


#include "store/benchmark/async/toy/toy_client.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <random>
#include <vector>

#include "lib/latency.h"
#include "lib/tcptransport.h"
#include "lib/timeval.h"
#include "store/benchmark/async/bench_client.h"

#include "store/common/frontend/sync_client.h"
#include "store/common/truetime.h"
#include "store/tapirstore/client.h"

#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include "store/common/query_result/query_result.h"
#include "lib/cereal/archives/binary.hpp"
#include "lib/cereal/types/string.hpp"

#include <fmt/core.h>

//#include "store/benchmark/async/json_table_writer.h"

namespace toy {

ToyTransaction::ToyTransaction(): SyncTransaction(10000) {
}

ToyTransaction::~ToyTransaction(){}

transaction_status_t ToyTransaction::Execute(SyncClient &client){
    return COMMITTED;
}

ToyClient::ToyClient(
     SyncClient &client, Transport &transport, uint64_t id,
    int numRequests, int expDuration, uint64_t delay, int warmupSec,
    int cooldownSec, int tputInterval, uint32_t abortBackoff, bool retryAborted,
    uint32_t maxBackoff, uint32_t maxAttempts, const uint32_t timeout, const std::string &latencyFilename)
    : SyncTransactionBenchClient(client, transport, id, numRequests,
                                 expDuration, delay, warmupSec, cooldownSec,
                                 tputInterval, abortBackoff, retryAborted, maxBackoff, maxAttempts, timeout,
                                 latencyFilename){
}

ToyClient::~ToyClient() {}

void ToyClient::ExecuteToy(){
    std::cerr << "Started client thread\n";
    //Calling directly into syncClient here. Usually SyncTransactionBenchClient calls SendNext, which generates a new transaction. This transaction then calls the operations on the SyncClient.
            uint32_t timeout = UINT_MAX;
            // client.Begin(timeout);
            // std::cerr << "Invoked Begin\n";
            // client.Put("y", "6", timeout);
            // client.Commit(timeout);
            // std::cerr << "Committed value for y\n";

           
            client.Begin(timeout);
            std::cerr << "Invoked Begin\n";
            client.Put("x", "5", timeout);
            std::string readValue;
            client.Get("x", readValue, timeout);
            std::cerr << "value read for x: " << readValue << "\n"; //Dummy read --> will read from buffered put; will not add to read set
              client.Get("y", readValue, timeout);
            std::cerr << "value read for x: " << readValue << "\n";
            client.Commit(timeout);
            std::cerr << "Committed value for x\n";

            
            client.Begin(timeout);

            std::string query = "SELECT *";
            const query_result::QueryResult* queryResult;
            client.Query(query, queryResult, timeout);  //--> Edit API in frontend sync_client.
                                           //For real benchmarks: Also edit in sync_transaction_bench_client.
                              
            // (*queryResult->at(0))[0] //TODO: parse the output...  data.length

            std::cerr << "Got res" << std::endl;
            UW_ASSERT(!queryResult->empty());
            std::cerr << "num cols:" <<  queryResult->columns() << std::endl;
            std::cerr << "num rows written:" <<  queryResult->rows_affected() << std::endl;
            std::cerr << "num rows read:" << queryResult->size() << std::endl;

             std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
            size_t nbytes;
            const char* out = queryResult->get(0, 0, &nbytes);
            std::string output(out, nbytes);
            ss << output;
            std::string output_row;
            {
              cereal::BinaryInputArchive iarchive(ss); // Create an input archive
              iarchive(output_row); // Read the data from the archive
            }
             std::cerr << "Query 1 Result: " << output_row << std::endl << std::endl;

            //Query 2: a point query:

            std::string p_query = fmt::format("SELECT * FROM datastore WHERE key_ = 'key'");
            const query_result::QueryResult* p_queryResult;
            client.Query(p_query, p_queryResult, timeout);  //--> Edit API in frontend sync_client.
                                           //For real benchmarks: Also edit in sync_transaction_bench_client.
                              
            // (*p_queryResult->at(0))[0] //TODO: parse the output...  data.length

            std::cerr << "Got res" << std::endl;
            UW_ASSERT(!p_queryResult->empty());
            std::cerr << "num cols:" <<  p_queryResult->columns() << std::endl;
            std::cerr << "num rows written:" <<  p_queryResult->rows_affected() << std::endl;
            std::cerr << "num rows read:" << p_queryResult->size() << std::endl;

             std::stringstream p_ss(std::ios::in | std::ios::out | std::ios::binary);
            nbytes;
            out = p_queryResult->get(0, 0, &nbytes);
            std::string p_output(out, nbytes);
            p_ss << p_output;
            output_row;
            {
              cereal::BinaryInputArchive iarchive(p_ss); // Create an input archive
              iarchive(output_row); // Read the data from the archive
            }
             std::cerr << "Query 2 Result: " << output_row << std::endl;


            client.Commit(timeout);
            std::cerr << "Committed Query\n";

            // sleep(1);
            
            // std::string query = "SELECT *";
            // std::string queryResult;
            // client.Query(query, queryResult, timeout);  //--> Edit API in frontend sync_client.
            //                                //For real benchmarks: Also edit in sync_transaction_bench_client.
            // std::cerr << "Query Result: " << queryResult << std::endl << std::endl;
            
}

 SyncTransaction *ToyClient::GetNextTransaction() {
    ToyTransaction *toyTx = new ToyTransaction();
  return toyTx;
}
std::string ToyClient::GetLastOp() const { return ""; }


}  // namespace smallbank
