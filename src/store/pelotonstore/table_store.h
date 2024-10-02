/***********************************************************************
 *
 * store/pequinstore/table_store_interface_peloton.h: 
 *      Implementation of a execution shim to pelton based backend.
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Neil Giridharan <giridhn@berkeley.edu>
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


#ifndef _TABLESTORE_H_
#define _TABLESTORE_H_

#include "lib/latency.h"
#include "lib/message.h"

// Include whatever Peloton Deps
// #include "../../query-engine/common/logger.h"
// #include "../../query-engine/common/macros.h"
// #include "../../query-engine/parser/drop_statement.h"
#include "./peloton/parser/postgresparser.h"
// #include "../../query-engine/traffic_cop/traffic_cop.h"

#include "./peloton/catalog/catalog.h"
// #include "../../query-engine/catalog/proc_catalog.h"
// #include "../../query-engine/catalog/system_catalogs.h"

#include "./peloton/concurrency/transaction_manager_factory.h"

// #include "../../query-engine/executor/create_executor.h"
// #include "../../query-engine/executor/create_function_executor.h"
// #include "../../query-engine/executor/executor_context.h"

// #include "../../query-engine/planner/create_function_plan.h"
// #include "../../query-engine/planner/create_plan.h"
// #include "../../query-engine/storage/data_table.h"

// #include "../../query-engine/executor/insert_executor.h"
// #include "../../query-engine/expression/constant_value_expression.h"
// #include "../../query-engine/parser/insert_statement.h"
// #include "../../query-engine/planner/insert_plan.h"
#include "./peloton/traffic_cop/traffic_cop.h"
// #include "../../query-engine/type/type.h"
// #include "../../query-engine/type/value_factory.h"
 #include "./peloton/optimizer/stats/stats_storage.h"


#include "store/common/query_result/query_result_proto_builder.h"
#include <ostream>
#include <string>
#include <tuple>

#include "tbb/concurrent_unordered_map.h"
#include <fmt/core.h>
#include <fmt/ranges.h>

namespace pelotonstore {

class TableStore {
    public:
        TableStore(int num_threads = 0);
        virtual ~TableStore();

        // Execute a statement as single statement transaction, no output
        void ExecSingle(const std::string &sql_statement, bool skip_cache = false);

        // Execute a statement that is part of the clients ongoing transaction
        std::string ExecTransactional(const std::string &sql_statement, uint64_t client_id, uint64_t tx_id, peloton_peloton::ResultType &result_status, std::string &error_msg, bool skip_cache = true);

        void Begin(uint64_t client_id, uint64_t tx_id);

        peloton_peloton::ResultType Commit(uint64_t client_id, uint64_t tx_id);
           
        void Abort(uint64_t client_id, uint64_t tx_id);
        
    private:
        void Init(int num_threads);

        std::vector<Latency_t> readLats;
        std::vector<Latency_t> writeLats;
        std::vector<Latency_t> snapshotLats;

        std::string unnamed_statement;
        bool unnamed_variable;

        //Peloton DB singleton "table_backend"
		peloton_peloton::tcop::TrafficCop traffic_cop_;
		std::atomic_int counter_;
        bool is_recycled_version_;

    
        tbb::concurrent_unordered_map<uint64_t, std::pair<peloton_peloton::tcop::TrafficCop*, std::atomic_int*>> client_cop; //map from client_id to traffic cop. 
        //Invariant: Client finishes its Txns sequentially. If not, need traffic cop per Transaction.

        int num_threads;
        std::vector<std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *>> traffic_cops_;
        std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> GetCop();
        std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> GetClientCop(uint64_t client_id, uint64_t tx_id);

        std::shared_ptr<peloton_peloton::Statement> ParseAndPrepare(const std::string &query_statement, peloton_peloton::tcop::TrafficCop *tcop, bool skip_cache = false);

        void GetResult(peloton_peloton::ResultType &status, uint64_t &rows_affected, peloton_peloton::tcop::TrafficCop *tcop, std::atomic_int *c);

        //std::string TransformResult(std::vector<peloton::FieldInfo> &tuple_descriptor, std::vector<peloton::ResultValue> &result);
        std::string TransformResult(peloton_peloton::ResultType &status, std::shared_ptr<peloton_peloton::Statement> statement, std::vector<peloton_peloton::ResultValue> &result, uint64_t rows_affected);
        

};

} // namespace pequinstore

#endif //_PELOTON_TABLESTORE_H