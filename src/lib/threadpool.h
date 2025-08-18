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
#ifndef _LIB_THREADPOOL_H_
#define _LIB_THREADPOOL_H_

#include "assert.h"
#include <list>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <condition_variable>
#include <thread>
#include <event2/event.h>
#include <deque>
#include "concurrentqueue/concurrentqueue.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
//#include "tbb/concurrent_queue.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_hash_map.h"

//using namespace tbb;

class ThreadPool {

public:

  ThreadPool();
  virtual ~ThreadPool();
  // copy constructor panics
  ThreadPool(const ThreadPool& tp) { Panic("Unimplemented"); }

  void start(int process_id = 0, int total_processes = 1, bool hyperthreading = true, bool server = true, int mode = 0, bool optimize_for_dev_machine = false);  // 0 = Indicus, 1 = Hotstuff, 2 = BFTSmart
  void stop();

  inline void cancel_load_threads(){load_running=false;}

  void dispatch(std::function<void*()> f, std::function<void(void*)> cb, event_base* libeventBase);
  void dispatch_local(std::function<void*()> f, std::function<void(void*)> cb);
  void detach(std::function<void*()> f);
  void detach_ptr(std::function<void*()> *f);
  void detach_main(std::function<void*()> f);
  void issueCallback(std::function<void(void*)> cb, void* arg, event_base* libeventBase);
  void issueMainThreadCallback(std::function<void(void*)> cb, void* arg);

  //void cancel_n_threads(uint64_t n);

  //Indexed threadpool
  void add_n_indexed(int num_threads); 
  void dispatch_indexed(uint64_t id, std::function<void *()> f, std::function<void(void *)> cb, event_base *libeventBase);
  void detach_indexed(uint64_t id, std::function<void *()> f); 

private:

  struct EventInfo {
      EventInfo(ThreadPool* tp): tp(tp) {}

      event* ev;
      std::function<void(void*)> cb;
      void* r;
      ThreadPool* tp;
  };

  static void EventCallback(evutil_socket_t fd, short what, void *arg);
  static void* combiner(std::function<void*()> f, std::function<void(void*)> cb);

  EventInfo* GetUnusedEventInfo();
  void FreeEventInfo(EventInfo *info);
  event* GetUnusedEvent(event_base* libeventBase, EventInfo* info);
  void FreeEvent(event* event);


  std::mutex EventInfoMutex;
  std::mutex EventMutex;
  std::condition_variable cv;
  std::vector<EventInfo*> eventInfos;
  std::vector<event*> events;
  
  bool running;
  bool load_running;
  std::vector<std::thread*> threads;

  moodycamel::BlockingConcurrentQueue<std::pair<std::function<void*()>, EventInfo*>> worker_thread_request_list;
  moodycamel::BlockingConcurrentQueue<std::function<void*()>> main_thread_request_list;

  //For indexed threadpool
  std::atomic_uint64_t total_indexed_workers = 0;
  // typedef tbb::concurrent_hash_map<uint64_t, moodycamel::BlockingConcurrentQueue<std::pair<std::function<void*()>, EventInfo*>>> IndexWorkerMap;
  typedef tbb::concurrent_unordered_map<uint64_t, moodycamel::BlockingConcurrentQueue<std::pair<std::function<void*()>, EventInfo*>>> IndexWorkerMap;
  IndexWorkerMap indexed_worker_thread_request_list;
};

#endif  // _LIB_THREADPOOL_H_
