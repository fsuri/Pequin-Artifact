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
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

namespace pequinstore {


void HandleQuery(const TransportAddress &remote, proto::Query &msg){
    //TODO: 
    // 1) Parse Message
    // 2) Authenticate Query. Confirm hash ID //TODO: Define Query hash, Sign Query Message
    // 3) Parse Query
    // 4) Execute all Scans in query --> find txnSet (for each key, last few tx)
    // 5) Create list of all txn-ids necessary for state
            // Use optimistic Tx-ids (= timestamp) if param set
    // 6) Compress list (only really doable if using optimistic IDs)
    // 7) Send list in SyncReply

    //How to store txnSet efficiently for key WITH RESPECT to Timestamp
    //Could already store a whole tx map for each key: map<key, deque<TxnIds>> --? replace tx_ids evertime a newer one comes along (pop front, push_back). Problem: May come in any TS order. AND: Query with TS only cares about TxId < TS
}

 
void HandleSync(const TransportAddress &remote, proto::SyncClientProposal &msg){
    // 1) Parse Message
    // 2) Authenticate Client
    // 3) Request any missing transactions (via txid) & add to state
            // Wait for up f+1 replies for each missing. (if none successful, then client must have been byz. Vote Early abort (if anything) and report client.)
    // 3.5) If optimistic ID maps to 2 txn-ids --> report issuing client (do this when you receive the tx already); vice versa, if we notice 2 optimistic ID's map to same tx --> report! 
        // (Can early abort query to not waste exec since sync might fail- or optimistically execute and hope for best) --> won't happen in simulation (unless testing failures)
    // 4) Execute Query -- Go through store, and check if latest tx in store is present in syncList. If it is missing one (committed) --> reply EarlyAbort (tx cannot succeed). If prepared is missing, ignore, skip to next
    // 5) Build Read Set while executing
    // 6) Create Merkle Tree over Read Set, result, query id 
    // 7) Cache Read Set
    // 8) If chosen for reply, create client reply with Result, Result_hash. 
    // 9) Sign Message 
    // 10) Send message
}

  
void HandleRequestTx(const TransportAddress &remote, proto::RequestMissingTxns &msg){
    //1) Parse Message
    //2) Check if requested tx present local (might not be if a byz sent request; or a byz client falsely told an honest replica which replicas have tx)
            // If using optimistic TX-id: map from optimistic ID to real txid to find Txn. (if present)  --> with optimistic ones we cant distinguish whether the sync client was byz and falsely suggested replica has tx
    //3) If present, reply to replica with it; If not, reply that it is not present (reply explicitly to catch byz clients). (if we want to avoid byz replicas falsely requesting, then clients would need to include 
                                                                                                                                        // signed snapshot vote. and we would have to forward it here to...)
                                                                                                                                        //can log requests, so a byz can request at most once (which is legal anyways)
    //4) Use MAC to authenticate own reply
    //5) If committed Tx, attach certificate
    //6) Send reply.
}

 
void HandleSupplyTx(const TransportAddress &remote, proto::SupplyMissingTxns &msg){
    // 1) Parse Message
    // 2) Check MAC authenticator
    // 3) check if Tx supplied or not
    // 4) If so, for committed tx: check certificate, for prepared tx: do OCC check
    // 5) 
}

//TODO: Should be split into Client and server code -- Make it a common file, or keep separate ones? (This is the servers file.)

} // namespace pequinstore