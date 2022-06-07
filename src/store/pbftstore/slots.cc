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
#include "store/pbftstore/slots.h"
#include "store/pbftstore/common.h"
#include "lib/crypto.h"

namespace pbftstore {

Slots::Slots() {}
Slots::~Slots() {}

bool Slots::setPreprepare(const proto::Preprepare &preprepare, uint64_t replica_id, const std::string& sig) {
  uint64_t view = preprepare.viewnum();
  uint64_t seq_num = preprepare.seqnum();
  std::string digest = preprepare.digest();

  if (slots[seq_num][view].preprepare.digest.empty()) {
    slots[seq_num][view].preprepare.digest = digest;
    slots[seq_num][view].preprepare.replica_id = replica_id;
    slots[seq_num][view].preprepare.sig = sig;
    // TODO could do GC on all prepares and commits without the digest
    return true;
  }
  return slots[seq_num][view].preprepare.digest == digest;
}

bool Slots::setPreprepare(const proto::Preprepare &preprepare) {
  // with no sigs just assume the preprepare came from the primary
  return setPreprepare(preprepare, 0, "");
}

bool Slots::addPrepare(const proto::Prepare &prepare, uint64_t replica_id, const std::string& sig) {
  uint64_t view = prepare.viewnum();
  uint64_t seq_num = prepare.seqnum();
  std::string digest = prepare.digest();

  if (slots[seq_num][view].preprepare.digest.empty() || slots[seq_num][view].preprepare.digest == digest) {
    slots[seq_num][view].prepares[digest][replica_id] = sig;
    return true;
  }
  return false;
}

bool Slots::addPrepare(const proto::Prepare &prepare) {
  uint64_t view = prepare.viewnum();
  uint64_t seq_num = prepare.seqnum();
  std::string digest = prepare.digest();

  // add a prepare with a fake id, don't really care because we don't have sigs
  return addPrepare(prepare, slots[seq_num][view].prepares[digest].size() + 1, "");
}

bool Slots::Prepared(uint64_t slot_num, uint64_t view, uint64_t f) {
  // first, we check if we got the preprepare message
  if (!slots[slot_num][view].preprepare.digest.empty()) {
    std::string digest = slots[slot_num][view].preprepare.digest;
    uint64_t preprepare_id = slots[slot_num][view].preprepare.replica_id;

    // check if we have enough prepare messages
    if (slots[slot_num][view].prepares[digest].find(preprepare_id) == slots[slot_num][view].prepares[digest].end()) {
      // if the preprepare replica did not submit a prepare message, we only need 2f
      return slots[slot_num][view].prepares[digest].size() >= 2*f;
    } else {
      // if the preprepare replica did submit a prepare message, we need 2f+1
      // to dedup the preprepare message
      return slots[slot_num][view].prepares[digest].size() >= 2*f + 1;
    }
  }

  return false;
}

bool Slots::addCommit(const proto::Commit &commit, uint64_t replica_id, const std::string& sig) {
  uint64_t view = commit.viewnum();
  uint64_t seq_num = commit.seqnum();
  std::string digest = commit.digest();

  if (slots[seq_num][view].preprepare.digest.empty() || slots[seq_num][view].preprepare.digest == digest) {
    slots[seq_num][view].commits[digest][replica_id] = sig;
    return true;
  }
  return false;
}

bool Slots::addCommit(const proto::Commit &commit) {
  uint64_t view = commit.viewnum();
  uint64_t seq_num = commit.seqnum();
  std::string digest = commit.digest();

  // add a commit with a fake id, don't really care because we don't have sigs
  return addCommit(commit, slots[seq_num][view].commits[digest].size(), "");
}

bool Slots::CommittedLocal(uint64_t seq_num, uint64_t view, uint64_t f) {
  if (Prepared(seq_num, view, f)) {
    // guaranteed to be valid by the Prepared predicate
    std::string digest = slots[seq_num][view].preprepare.digest;

    return slots[seq_num][view].commits[digest].size() >= 2*f + 1;
  }
  return false;
}

std::string Slots::getSlotDigest(uint64_t seq_num, uint64_t view) {
  return slots[seq_num][view].preprepare.digest;
}

proto::GroupedSignedMessage Slots::getPrepareProof(uint64_t seq_num, uint64_t view, const std::string& digest) {
  proto::Prepare prepare;
  prepare.set_seqnum(seq_num);
  prepare.set_viewnum(view);
  prepare.set_digest(digest);

  proto::PackedMessage packedMsg;
  *packedMsg.mutable_msg() = prepare.SerializeAsString();
  *packedMsg.mutable_type() = prepare.GetTypeName();
  std::string msgData = packedMsg.SerializeAsString();

  proto::GroupedSignedMessage proof;
  proof.set_packed_msg(msgData);

  for (const auto& pair : slots[seq_num][view].prepares[digest]) {
    (*proof.mutable_signatures())[pair.first] = pair.second;
  }

  return proof;
}

proto::GroupedSignedMessage Slots::getCommitProof(uint64_t seq_num, uint64_t view, const std::string& digest) {
  proto::Commit commit;
  commit.set_seqnum(seq_num);
  commit.set_viewnum(view);
  commit.set_digest(digest);

  proto::PackedMessage packedMsg;
  *packedMsg.mutable_msg() = commit.SerializeAsString();
  *packedMsg.mutable_type() = commit.GetTypeName();
  std::string msgData = packedMsg.SerializeAsString();

  proto::GroupedSignedMessage proof;
  proof.set_packed_msg(msgData);

  for (const auto& pair : slots[seq_num][view].commits[digest]) {
    (*proof.mutable_signatures())[pair.first] = pair.second;
  }

  return proof;
}

}  // namespace pbftstore
