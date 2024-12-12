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
#ifndef SINTR_SHARED_BATCH_SIGNER_H
#define SINTR_SHARED_BATCH_SIGNER_H

#include <condition_variable>
#include <functional>
#include <queue>
#include <thread>
#include <vector>

#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/named_condition.hpp>
#include <boost/interprocess/containers/deque.hpp>
#include <boost/interprocess/containers/string.hpp>

#include "lib/transport.h"
#include "lib/keymanager.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include "store/sintrstore/common.h"
#include "store/common/stats.h"
#include "store/sintrstore/batchsigner.h"

namespace sintrstore {

using namespace boost::interprocess;

class SharedBatchSigner : public BatchSigner {
 public:
  SharedBatchSigner(Transport *transport, KeyManager *keyManager, Stats &stats,
      uint64_t batchTimeoutMicro, uint64_t batchSize, uint64_t id,
      bool adjustBatchSize, uint64_t merkleBranchFactor);
  virtual ~SharedBatchSigner();

  virtual void MessageToSign(::google::protobuf::Message* msg,
      proto::SignedMessage *signedMessage, signedCallback cb,
      bool finishBatch = false) override;

  virtual void asyncMessageToSign(::google::protobuf::Message* msg,
          proto::SignedMessage *signedMessage, signedCallback cb,
          bool finishBatch = false) override;

  //Latency_t waitOnBatchLock;

 private:
  void BatchTimeout();
  void SignBatch();

  void StopTimeout();
  void StartTimeout();

  void RunSignedCallbackConsumer();
  void RunSignTimeoutChecker();

  uint64_t batchSize;
  std::mutex batchTimerMtx;
  int batchTimerId;

  std::mutex pendingBatchMtx;
  struct PendingBatchItem {
    PendingBatchItem(uint64_t id, signedCallback cb,
        proto::SignedMessage *signedMessage) : id(id), cb(cb),
        signedMessage(signedMessage) {
    }

    uint64_t id;
    signedCallback cb;
    proto::SignedMessage *signedMessage;
  };
  uint64_t nextPendingBatchId;
  std::map<uint64_t, PendingBatchItem> pendingBatch;


  bool alive;
  std::thread *signedCallbackThread;

  typedef allocator<void, managed_shared_memory::segment_manager> void_allocator;
  typedef allocator<char, managed_shared_memory::segment_manager> CharAllocator;
  typedef basic_string<char, std::char_traits<char>, CharAllocator> MyShmString;
  typedef allocator<MyShmString, managed_shared_memory::segment_manager> StringAllocator;

  struct SignatureWork {
    MyShmString data;
    uint64_t pid;
    uint64_t id;

    SignatureWork(const void_allocator &void_alloc, const char * data, uint64_t dataLen,
        uint64_t pid, uint64_t id) : data(data, dataLen, void_alloc), pid(pid), id(id) { }
  };

  typedef allocator<SignatureWork, managed_shared_memory::segment_manager> ShmemAllocator;
  typedef deque<SignatureWork, ShmemAllocator> SignatureWorkQueue;
  managed_shared_memory *segment;
  const void_allocator *alloc_inst;

  int currentBatchId;
  int *sharedBatchId;
  named_mutex *sharedWorkQueueMtx;
  named_condition *sharedWorkQueueCond;
  SignatureWorkQueue *sharedWorkQueue;

  named_mutex *GetCompletionQueueMutex(uint64_t id);
  named_condition *GetCompletionQueueCondition(uint64_t id);
  SignatureWorkQueue *GetCompletionQueue(uint64_t id);

  std::map<uint64_t, named_mutex *> completionQueueMtx;
  std::map<uint64_t, named_condition *> completionQueueReady;
  std::map<uint64_t, SignatureWorkQueue *> completionQueues;




};

} // namespace sintrstore

#endif /* SINTR_SHARED_BATCH_SIGNER_H */
