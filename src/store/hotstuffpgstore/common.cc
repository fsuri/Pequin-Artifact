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
#include "store/hotstuffpgstore/common.h"

#include <cryptopp/sha.h>
#include <unordered_set>
#include <thread>
#include <atomic>

#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/hotstuffpgstore/pbft_batched_sigs.h"

namespace hotstuffpgstore {

using namespace CryptoPP;

bool ValidateSignedMessage(const proto::SignedMessage &signedMessage,
    KeyManager *keyManager, ::google::protobuf::Message &plaintextMsg) {
  proto::PackedMessage packedMessage;
  if (!__PreValidateSignedMessage(signedMessage, keyManager, packedMessage)) {
    return false;
  }

  if (packedMessage.type() != plaintextMsg.GetTypeName()) {
    return false;
  }

  plaintextMsg.ParseFromString(packedMessage.msg());
  return true;
}

bool ValidateSignedMessage(const proto::SignedMessage &signedMessage,
    KeyManager *keyManager, std::string &data, std::string &type) {
  proto::PackedMessage packedMessage;
  if (!__PreValidateSignedMessage(signedMessage, keyManager, packedMessage)) {
    return false;
  }

  data = packedMessage.msg();
  type = packedMessage.type();
  return true;
}

bool __PreValidateSignedMessage(const proto::SignedMessage &signedMessage,
    KeyManager *keyManager, proto::PackedMessage &packedMessage) {
  if (!CheckSignature(signedMessage, keyManager)) {
    return false;
  }

  return packedMessage.ParseFromString(signedMessage.packed_msg());
}

bool CheckSignature(const proto::SignedMessage &signedMessage,
    KeyManager *keyManager) {
    crypto::PubKey* replicaPublicKey = keyManager->GetPublicKey(
        signedMessage.replica_id());
    // verify that the replica actually sent this reply and that we are expecting
    // this reply
    return crypto::IsMessageValid(replicaPublicKey, signedMessage.packed_msg(),
          &signedMessage);
}

void SignMessage(const ::google::protobuf::Message &msg,
    crypto::PrivKey* privateKey, uint64_t processId,
    proto::SignedMessage &signedMessage) {
  proto::PackedMessage packedMsg;
  *packedMsg.mutable_msg() = msg.SerializeAsString();
  *packedMsg.mutable_type() = msg.GetTypeName();
  // TODO this is not portable. SerializeAsString may not return the same
  // result every time
  std::string msgData = packedMsg.SerializeAsString();
  crypto::SignMessage(privateKey, msgData, signedMessage);
  signedMessage.set_packed_msg(msgData);
  signedMessage.set_replica_id(processId);
}

template <typename S>
void PackRequest(proto::PackedMessage &packedMsg, S &s) {
  packedMsg.set_msg(s.SerializeAsString());
  packedMsg.set_type(s.GetTypeName());
}

std::string TransactionDigest(const proto::Transaction &txn) {
  CryptoPP::SHA256 hash;
  std::string digest;

  for (const auto &group : txn.participating_shards()) {
    hash.Update((const CryptoPP::byte*) &group, sizeof(group));
  }
  for (const auto &read : txn.readset()) {
    uint64_t readtimeId = read.readtime().id();
    uint64_t readtimeTs = read.readtime().timestamp();
    hash.Update((const CryptoPP::byte*) &read.key()[0], read.key().length());
    hash.Update((const CryptoPP::byte*) &readtimeId,
        sizeof(read.readtime().id()));
    hash.Update((const CryptoPP::byte*) &readtimeTs,
        sizeof(read.readtime().timestamp()));
  }
  for (const auto &write : txn.writeset()) {
    hash.Update((const CryptoPP::byte*) &write.key()[0], write.key().length());
    hash.Update((const CryptoPP::byte*) &write.value()[0], write.value().length());
  }
  uint64_t timestampId = txn.timestamp().id();
  uint64_t timestampTs = txn.timestamp().timestamp();
  hash.Update((const CryptoPP::byte*) &timestampId,
      sizeof(timestampId));
  hash.Update((const CryptoPP::byte*) &timestampTs,
      sizeof(timestampTs));

  digest.resize(hash.DigestSize());
  hash.Final((CryptoPP::byte*) &digest[0]);

  return digest;
}

std::string BatchedDigest(proto::BatchedRequest& breq) {

  CryptoPP::SHA256 hash;
  std::string digest;

  for (int i = 0; i < breq.digests_size(); i++) {
    std::string dig = (*breq.mutable_digests())[i];
    hash.Update((CryptoPP::byte*) &dig[0], dig.length());
  }

  digest.resize(hash.DigestSize());
  hash.Final((CryptoPP::byte*) &digest[0]);

  return digest;
}

std::string string_to_hex(const std::string& input)
{
    static const char hex_digits[] = "0123456789ABCDEF";

    std::string output;
    output.reserve(input.length() * 2);
    for (unsigned char c : input)
    {
        output.push_back(hex_digits[c >> 4]);
        output.push_back(hex_digits[c & 15]);
    }
    return output;
}

void DebugHash(const std::string& hash) {
  std::string hex_hash =string_to_hex(hash);
  Debug("Hash: %s", hex_hash.substr(hex_hash.size() - 10).c_str());
  // Debug("Hash: %s", string_to_hex(hash).substr(0,10).c_str());
}


} // namespace indicusstore
