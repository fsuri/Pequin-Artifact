/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Yunhao Zhang <yz2327@cornell.edu>
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
#include "store/hotstuffpgstore/shardclient.h"
#include "store/hotstuffpgstore/pbft_batched_sigs.h"
#include "store/hotstuffpgstore/common.h"

namespace hotstuffpgstore {

ShardClient::ShardClient(const transport::Configuration& config, Transport *transport,
    uint64_t client_id, uint64_t group_idx, const std::vector<int> &closestReplicas_,
    bool signMessages, bool validateProofs,
    KeyManager *keyManager, Stats* stats, bool order_commit, bool validate_abort, bool deterministic) :
    config(config), transport(transport),
    group_idx(group_idx),
    signMessages(signMessages), validateProofs(validateProofs),
    keyManager(keyManager), stats(stats), order_commit(order_commit), validate_abort(validate_abort),
    deterministic(deterministic) {
  transport->Register(this, config, -1, -1);
  readReq = 0;
  inquiryReq = 0;
  applyReq = 0;

  if (closestReplicas_.size() == 0) {
    for  (int i = 0; i < config.n; ++i) {
      closestReplicas.push_back((i + client_id) % config.n);
      // Debug("i: %d; client_id: %d", i, client_id);
      // Debug("Calculations: %d", (i + client_id) % config->n);
    }
  } else {
    closestReplicas = closestReplicas_;
  }

}

ShardClient::~ShardClient() {}

void ShardClient::ReceiveMessage(const TransportAddress &remote, const std::string &t, const std::string &d, void *meta_data) {

  Debug("handling message of type %s", t.c_str());
  proto::SignedMessage signedMessage;
  std::string type;
  std::string data;
  proto::InquiryReply inquiryReply;
  proto::ApplyReply applyReply;

  bool recvSignedMessage = false;
  if (t == signedMessage.GetTypeName()) {
    if (!signedMessage.ParseFromString(d)) {
      return;
    }

    proto::PackedMessage pmsg;
    pmsg.ParseFromString(signedMessage.packed_msg());
    std::cout << "Inner type: " << pmsg.type() << std::endl;
    if (pmsg.type() == inquiryReply.GetTypeName() || pmsg.type() == applyReply.GetTypeName() ) {
      crypto::PubKey* replicaPublicKey = keyManager->GetPublicKey(signedMessage.replica_id());
      if (!hotstuffpgBatchedSigs::verifyBatchedSignature(signedMessage.mutable_signature(), signedMessage.mutable_packed_msg(),
            replicaPublicKey)) {
             Debug("dec signature was invalid");
             return;
            }
      data = pmsg.msg();
      type = pmsg.type();
    } else if(!ValidateSignedMessage(signedMessage, keyManager, data, type)) {
       Debug("signature was invalid");
       return;
    }

    recvSignedMessage = true;
    Debug("signature was valid");
  } else {
    type = t;
    data = d;
  }

  if (type == inquiryReply.GetTypeName()) {
    inquiryReply.ParseFromString(data);
    if(signMessages && !recvSignedMessage) {
      return;
    }
    HandleInquiryReply(inquiryReply, signedMessage);

  } else if (type == applyReply.GetTypeName()) {
    applyReply.ParseFromString(data);
    if(signMessages && !recvSignedMessage) {
      return;
    }
    HandleApplyReply(applyReply, signedMessage);
  }
}

// ================================
// ======= MESSAGE HANDLERS =======
// ================================

void ShardClient::HandleInquiryReply(const proto::InquiryReply& inquiryReply, const proto::SignedMessage& signedMsg) {
  Debug("Handling an inquiry reply");

  uint64_t reqId = inquiryReply.req_id();
  Debug("Inquiry req id: %lu", reqId);

  if(pendingInquiries.find(reqId) != pendingInquiries.end()) {
    PendingInquiry* pendingInquiry = &pendingInquiries[reqId];

    pendingInquiry->numReceivedReplies++;
    Debug("Shir: got an additional reply. request:  %lu now has  %lu replies.",reqId,pendingInquiry->numReceivedReplies);
    if(signMessages) { // May have to just take the second path?
      uint64_t replica_id = signedMsg.replica_id();
      if (replica_id / config.n != (uint64_t) group_idx) {
        Debug("Inquiry Reply: replica not in group");
        return;
      }
      if(inquiryReply.status() == REPLY_OK) {
        pendingInquiry->receivedReplies[inquiryReply.sql_res()].insert(replica_id);
        Debug("Shir: query reply was OK");

        // Timestamp its(inquiryReply.value_timestamp());
        if(pendingInquiry->status == REPLY_FAIL) {
          Debug("Updating inquiry reply signed");
          // pendingInquiry->maxTs = its;
          pendingInquiry->status = REPLY_OK;
          Debug("Shir: 222");

        }
        if(!deterministic && signMessages) {
          pendingInquiry->receivedSuccesses.insert(replica_id);
          Debug("Shir: 333");

          // std::cerr<<"Shir: query result:     "<<inquiryReply.sql_res() <<"\n";
          if(replica_id == 0) {
            Debug("Shir: 444");

            pendingInquiry->leaderReply = inquiryReply.sql_res();
          }
        }
      } else {
        Debug("Shir: 555");

        pendingInquiry->receivedFails.insert(replica_id);
        if(!deterministic && signMessages && replica_id == 0) {
          InquiryReplyHelper(pendingInquiry, inquiryReply.sql_res(), reqId, REPLY_FAIL);
        }
      }



    } else {
      Debug("Shir: 666");

      if(inquiryReply.status() == REPLY_OK) {
        Debug("Shir: 777");

        pendingInquiry->receivedReplies[inquiryReply.sql_res()].insert(pendingInquiry->numReceivedReplies);
        // Timestamp its(inquiryReply.value_timestamp());
        if(pendingInquiry->status == REPLY_FAIL) {
          Debug("Updating inquiry reply");
          // pendingInquiry->maxTs = its;
          pendingInquiry->status = REPLY_OK;
        }
      
      } else {
        pendingInquiry->receivedFails.insert(pendingInquiry->numReceivedReplies);
      }
    }

    Debug("Shir: 888");
    std::cerr <<"Shir: Is deterministic?:  "<< deterministic <<"\n";
    std::cerr <<"Shir: Is signed messages?:  "<< signMessages <<"\n";
    if(!signMessages || deterministic) { // This is for a fault tolerant system, curently we only look for the leader's opinion (only works in signed system)
      Debug("Shir: 999");
      if(pendingInquiry->receivedReplies[inquiryReply.sql_res()].size() 
          >= (uint64_t) config.f + 1) {
        InquiryReplyHelper(pendingInquiry, inquiryReply.sql_res(), reqId, pendingInquiry->status);
      } else if(pendingInquiry->receivedReplies.size() + pendingInquiry->receivedFails.size() 
          >= (uint64_t) config.f + 1) {
        InquiryReplyHelper(pendingInquiry, inquiryReply.sql_res(), reqId, REPLY_FAIL);
      }
    } else {
      Debug("Shir: 101010");
      if(pendingInquiry->receivedSuccesses.size() >= (uint64_t) config.f + 1 && 
          pendingInquiry->receivedSuccesses.find(0) != pendingInquiry->receivedSuccesses.end()) {
        InquiryReplyHelper(pendingInquiry, pendingInquiry->leaderReply, reqId, pendingInquiry->status);
      }
    }
  }
}

void ShardClient::InquiryReplyHelper(PendingInquiry* pendingInquiry, const std::string inquiryRes, 
    uint64_t reqId, uint64_t status) {
  if(pendingInquiry->timeout != nullptr) {
    pendingInquiry->timeout->Stop();
  }
  inquiry_callback icb = pendingInquiry->icb;
  pendingInquiries.erase(reqId);
  icb(status, inquiryRes);
}

void ShardClient::HandleApplyReply(const proto::ApplyReply& applyReply, const proto::SignedMessage& signedMsg) {
  Debug("Handling an apply reply");

  uint64_t reqId = applyReply.req_id();
  Debug("Apply req id: %lu", reqId);

  if(pendingApplies.find(reqId) != pendingApplies.end()) {
    PendingApply* pendingApply = &pendingApplies[reqId];

    uint64_t replica_id = pendingApply->receivedAcks.size() + pendingApply->receivedFails.size();
    if (signMessages) {
      replica_id = signedMsg.replica_id();
    }
    
    if(applyReply.status() == REPLY_OK) {
      pendingApply->receivedAcks.insert(replica_id);
      // Timestamp its(inquiryReply.value_timestamp());
    } else {
      pendingApply->receivedFails.insert(replica_id);
      if(!deterministic && signMessages && replica_id == 0) {
        if(pendingApply->timeout != nullptr) {
          pendingApply->timeout->Stop();
        }
        apply_callback acb = pendingApply->acb;
        pendingApplies.erase(reqId);
        acb(REPLY_FAIL);
      }
    }

    if(!signMessages || deterministic) {
      if(pendingApply->receivedAcks.size() >= (uint64_t) config.f + 1) {
        if(pendingApply->timeout != nullptr) {
          pendingApply->timeout->Stop();
        }
        apply_callback acb = pendingApply->acb;
        pendingApplies.erase(reqId);
        acb(REPLY_OK);
      } else if(pendingApply->receivedFails.size() >= (uint64_t) config.f + 1) {
        if(pendingApply->timeout != nullptr) {
          pendingApply->timeout->Stop();
        }
        apply_callback acb = pendingApply->acb;
        pendingApplies.erase(reqId);
        acb(REPLY_FAIL);
      }
    } else {
      if(pendingApply->receivedAcks.size() >= (uint64_t) config.f + 1 && 
      pendingApply->receivedAcks.find(0) != pendingApply->receivedAcks.end()) {
        if(pendingApply->timeout != nullptr) {
          pendingApply->timeout->Stop();
        }
        apply_callback acb = pendingApply->acb;
        pendingApplies.erase(reqId);
        acb(REPLY_OK);
      }
    }
  }
}


// ================================
// ==== SHARD CLIENT INTERFACE ====
// ================================

// Currently assumes no duplicates, can add de-duping code later if needed
void ShardClient::Query(const std::string &query,  const Timestamp &ts, uint64_t client_id, int client_seq_num, 
      inquiry_callback icb, inquiry_timeout_callback itcb,  uint32_t timeout) {

  proto::Inquiry inquiry;

  uint64_t reqId = inquiryReq++;
  Debug("Query id: %lu", reqId);

  inquiry.set_req_id(reqId);
  inquiry.set_query(query);
  inquiry.set_client_id(client_id);
  inquiry.set_txn_seq_num(client_seq_num);
  ts.serialize(inquiry.mutable_timestamp());

  proto::Request request;
  request.set_digest(crypto::Hash(inquiry.SerializeAsString()));
  request.mutable_packed_msg()->set_msg(inquiry.SerializeAsString());
  request.mutable_packed_msg()->set_type(inquiry.GetTypeName());


  Debug("Sending Query id: %lu", reqId);

  transport->SendMessageToGroup(this, group_idx, request);

  PendingInquiry pi;
  pi.icb = icb;
  pi.status = REPLY_FAIL;
  pi.numReceivedReplies = 0;
  pi.leaderReply = "";
  // pi.maxTs = Timestamp();
  pi.timeout = new Timeout(transport, timeout, [this, reqId, itcb]() {
    Debug("Query timeout called (but nothing was done)");
      stats->Increment("q_tout", 1);
      fprintf(stderr,"q_tout recv %lu\n",  (uint64_t) config.f + 1);
  });
  pi.timeout->Start();

  pendingInquiries[reqId] = pi;

}

void ShardClient::Commit(const std::string& txn_digest, const Timestamp &ts, uint64_t client_id, int client_seq_num, 
  apply_callback acb, apply_timeout_callback atcb, uint32_t timeout) {


  Debug("Shir: shardClient trying to commit the txn");
  proto::Apply apply;

  uint64_t reqId = applyReq++;
  Debug("Commit id: %lu", reqId);

  apply.set_req_id(reqId);
  apply.set_client_id(client_id);
  apply.set_txn_seq_num(client_seq_num);
  ts.serialize(apply.mutable_timestamp());
  Debug("Shir: a");

  proto::Request request;
  request.set_digest(txn_digest);
  Debug("Shir: b");

  request.mutable_packed_msg()->set_msg(apply.SerializeAsString());
  request.mutable_packed_msg()->set_type(apply.GetTypeName());
  

  Debug("Shir: now going to send a message in order to do that");
  transport->SendMessageToGroup(this, group_idx, request);

  Debug("Shir: trying to send the following message:");
  std::cerr<< "To group:   "<<group_idx<<"   Send the request:  "<<apply.GetTypeName() <<"\n";

  PendingApply pa;
  pa.acb = acb;
  pa.timeout = new Timeout(transport, timeout, [this, reqId, atcb](){
    Debug("Commit timeout called (but nothing was done)");
      stats->Increment("c_tout", 1);
      fprintf(stderr,"c_tout recv %lu\n",  (uint64_t) config.f + 1);
  });
  pa.timeout->Start();

  pendingApplies[reqId] = pa;
}

void ShardClient::Abort(const std::string& txn_digest,  uint64_t client_id, int client_seq_num) {

  proto::Rollback rollback;

  Debug("Abort Triggered");

  rollback.set_client_id(client_id);
  rollback.set_txn_seq_num(client_seq_num);

  proto::Request request;
  request.set_digest(txn_digest);
  request.mutable_packed_msg()->set_msg(rollback.SerializeAsString());
  request.mutable_packed_msg()->set_type(rollback.GetTypeName());

  transport->SendMessageToGroup(this, group_idx, request);
}

}
