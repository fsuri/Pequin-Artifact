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
#include "lib/threadpool.h"

#include <iostream>
#include <sched.h>
#include <sys/sysinfo.h>
#include <thread>
#include <utility>


ThreadPool::ThreadPool() {}

static bool use_load_bonus = true; //TODO: Make this an input param

void ThreadPool::start(int process_id, int total_processes, bool hyperthreading,
                       bool server, int mode, bool optimize_for_dev_machine) {              
  // printf("starting threadpool \n");
  // if hardware_concurrency is wrong try this:
  cpu_set_t cpuset;
  sched_getaffinity(0, sizeof(cpuset), &cpuset);
  Notice("cpu_count  %d \n", CPU_COUNT(&cpuset));
  Notice("get_nprocs  %d \n", get_nprocs());

  // could pre-allocate some Events and EventInfos for a Hotstart
  if (server) {
    Notice("starting server threadpool\n");
    Notice("process_id: %d, total_processes: %d \n", process_id, total_processes);
    // TODO: add config param for hyperthreading
    // bool hyperthreading = true;

    //int num_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    int num_cpus = std::thread::hardware_concurrency(); ///(2-hyperthreading);

    Notice("Total Num_cpus on server: %d \n", num_cpus);

    bool put_all_threads_on_same_core = false;

    if (optimize_for_dev_machine){
      num_cpus = 16;
      Notice("(Using Dev Machine: Total Num_cpus on server downregulated to: %d \n", num_cpus);
    } 
    if (num_cpus > 8 && !optimize_for_dev_machine) {
      num_cpus = 8;
      Notice("Total Num_cpus on server downregulated to: %d \n", num_cpus);
    }

    num_cpus /= total_processes;
    Notice("Num_cpus used for replica #%d: %d \n", process_id, num_cpus);
    int offset = process_id * num_cpus; // Offset that determines where first
                                        // core of the server begins.
    uint32_t num_threads = (uint32_t)std::max(1, num_cpus);
    // Currently: First CPU = MainThread.

    if (num_threads < 3)
      put_all_threads_on_same_core = true; // Network thread, main thread, and
                                           // worker threads start on same core.

    uint32_t start = 1 - put_all_threads_on_same_core; // First core
    uint32_t end = num_threads;                        // Last core
    Notice("Threadpool threads: start %d, end %d \n", start, end);
    if (mode == 0) { // Indicus
      // Use defaults. First core is messagine (inactive in threadpool), second
      // is Main Logic Thread, remainder are workers (crypto/reads/asynchronous
      // handling)
    } else if (mode == 1) { // TxHotstuff
      int num_core_for_hotstuff = 0;
      // if (total_processes <= 2) {
      //     num_core_for_hotstuff = 1;
      // } else {
      //     num_core_for_hotstuff = 0;
      // }
      end = end - num_core_for_hotstuff; // use last core for Hotstuff only
    } else if (mode == 2) {              // TxBFTSmart && Pequin
      start = 0;                         // use all cores
    } else
      Panic("No valid system defined");

    Debug("Network Process running on CPU %d.", sched_getcpu());
    running = true;
    
    if(use_load_bonus){
      load_running = true;
    }

    Notice("Threadpool running with %d main thread, and %d worker threads \n", 1, end-start);
    for (uint32_t i = start - use_load_bonus; i < end; i++) {
      UW_ASSERT(i >= 0);
      std::thread *t;

      // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
      // only CPU i as set.
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(i + offset, &cpuset);
      if (i + offset > 7 && !optimize_for_dev_machine)
        return; // XXX This is a hack to support the non-crypto experiment that does not actually use multiple cores

      // Mainthread   -- this is ONE thread on which any workload that must be sequential should be run.
      if (i == start) { // if run with >=3 cores then start == 1; If cores < 3,
                        // start == 0 -->run main_thread on first core.
        t = new std::thread([this, i] {
          while (true) {
            if (!running) {
              break;
            }

            std::function<void *()> job;

            main_thread_request_list.wait_dequeue(job);
             Debug("Main Thread %d running job on CPU %d", i, sched_getcpu());
            //Notice("Main Thread %d running job on CPU %d", i, sched_getcpu());

            job();
          }
        });
        Notice("THREADPOOL SETUP: Trying to pin main thread %d to core %d", i, (i+offset) );
        int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t),  &cpuset);
        if (rc != 0) {
          Panic("Error calling pthread_setaffinity_np: %d", rc);
        }
        threads.push_back(t);
        t->detach();
      }
      // Worker Thread -- these threads are best used to perform asynchronous jobs (e.g. Reads or Crypto)
      if ((i + put_all_threads_on_same_core) > start || use_load_bonus) { // if run with >=3 cores: start==1 &
          // put_all_threads_on_same_core == 0 --> workers start on cores 2+; if < 3 cores: start == 0, put_all = 1

        bool is_load_only_thread = !((i + put_all_threads_on_same_core) > start); //if the thread would not have been started without load_bonus.

        t = new std::thread([this, i, is_load_only_thread] {
          while (true) {
            if(is_load_only_thread && !load_running){ 
              Notice("Turning off load-only worker thread %d", i);
              break;
            }
            
            if (!running) {
              break;
            }

            std::pair<std::function<void *()>, EventInfo *> job;
            worker_thread_request_list.wait_dequeue(job);
           // Debug("popped job on CPU %d.", i);
            Debug("Worker Thread %d running job on CPU %d", i, sched_getcpu());
            //Notice("Worker Thread %d running job on CPU %d", i, sched_getcpu());
           
            if (job.second) {
              job.second->r = job.first();
              // This _should_ be thread safe
              event_active(job.second->ev, 0, 0);
            } else {
              job.first();
            }
          }
        });
        Notice("THREADPOOL SETUP: Trying to pin worker thread %d to core %d. Load only? %d", i, (i+offset), is_load_only_thread);
        int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
          Panic("Error calling pthread_setaffinity_np: %d", rc);
        }
        threads.push_back(t);
        t->detach();
      }

      // std::cerr << "THREADPOOL SETUP: Trying to pin thread to core: " << i << " + " << offset << std::endl; 
      // int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuset);
      // if (rc != 0) {
      //     Panic("Error calling pthread_setaffinity_np: %d", rc);
      // }
      // threads.push_back(t);
      // t->detach();
      //Notice("Finished server-side threadpool configurations");
    }
  }
  //CLIENT THREADPOOL 
  else {
    fprintf(stderr, "starting client threadpool\n");
    int num_cpus = std::thread::hardware_concurrency(); ///(2-hyperthreading);
    fprintf(stderr, "Num_cpus: %d \n", num_cpus);
    if (num_cpus > 8) {
      num_cpus = 8;
      fprintf(stderr, "Total Num_cpus on client downregulated to: %d \n", num_cpus);
    }
    // Note: Each client uses all 8 cores for additional workers. 
    // (However, by default we run with client_multithreading off though, so they are unused.) num_cpus /= total_processes;   
    // Note: Use this if one wants to dedicate a certain number of threads per client.

    Debug("num cpus per process: %d", num_cpus);
    uint32_t num_threads = (uint32_t)std::max(1, num_cpus);
    running = true;
    for (uint32_t i = 0; i < num_threads; i++) {
      std::thread *t;
      t = new std::thread([this, i] {
        while (true) {
          std::pair<std::function<void *()>, EventInfo *> job;

          Debug("Thread %d running on CPU %d.", i, sched_getcpu());
          worker_thread_request_list.wait_dequeue(job);

          if (!running) {
            break;
          }

          if (job.second) {
            job.second->r = job.first();
            event_active(job.second->ev, 0, 0);
          } else {
            job.first();
          }
        }
      });
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(i, &cpuset);
      int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t),
                                      &cpuset);

      if (rc != 0) {
        Panic("Error calling pthread_setaffinity_np: %d", rc);
      }
      Debug("MainThread running on CPU %d.", sched_getcpu());
      threads.push_back(t);
      t->detach();
    }
  }
}

ThreadPool::~ThreadPool() { stop(); }

void ThreadPool::stop() {
  running = false;

  // for(auto t: threads){
  //    t->join();
  //    delete t;
  // }
}

void ThreadPool::EventCallback(evutil_socket_t fd, short what, void *arg) {
  // we want to run the callback in the main event loop
  EventInfo *info = (EventInfo *)arg;
  info->cb(info->r);

  info->tp->FreeEvent(info->ev);
  info->tp->FreeEventInfo(info);
}

// void ThreadPool::cancel_load_threads(uint64_t n){
//   n = std::min(n, threads.size());
//   auto f = [this, n](){
//     for(int i = 0; i < n; ++i){
//       pthread_cancel(threads[i]->native_handle());
//     }
//     return (void*) true;
//   };
//   detach_main(f);
// }

//Dispatch a job f to the worker threads. Once complete, we will issue a callback cb on the process eventBase
void ThreadPool::dispatch(std::function<void *()> f,
                          std::function<void(void *)> cb,
                          event_base *libeventBase) {

  EventInfo *info = GetUnusedEventInfo();
  info->cb = std::move(cb);
  info->ev = GetUnusedEvent(libeventBase, info);
  event_add(info->ev, NULL);

  worker_thread_request_list.enqueue(std::make_pair(std::move(f), info));
}

void *ThreadPool::combiner(std::function<void *()> f,
                           std::function<void(void *)> cb) {
  cb(f());
  return nullptr;
}

//Dispatch a job f to the worker threads. Once complete, the worker thread itself will locally call a followup callback cb
void ThreadPool::dispatch_local(std::function<void *()> f,
                                std::function<void(void *)> cb) {
  EventInfo *info = nullptr;
  auto combination = [f = std::move(f), cb = std::move(cb)]() {
    cb(f());
    return nullptr;
  };

  worker_thread_request_list.enqueue(std::make_pair(std::move(combination), info));
}

//Dispatch a job f to the worker thread, without any callback.
void ThreadPool::detach(std::function<void *()> f) {
  EventInfo *info = nullptr;

  worker_thread_request_list.enqueue(std::make_pair(std::move(f), info));
}

//Dispatch a job f to the worker thread, without any callback. Take f as pointer
void ThreadPool::detach_ptr(std::function<void *()> *f) {
  EventInfo *info = nullptr;

  worker_thread_request_list.enqueue(std::make_pair(std::move(*f), info));
}

//Dispatch a job f to the main (serial) thread, without any callback.
void ThreadPool::detach_main(std::function<void *()> f) {
  EventInfo *info = nullptr;

  main_thread_request_list.enqueue(std::move(f));
}

////////////////////////////////
// requires transport object to call this... (add to the verifyObj)
// could alternatively use:
//  transport->Timer(0, f)   // expects a timer_callback_t though, which is a
//  void(void) typedef
// could make f purely void, if I refactored a bunch
// lazy solution:
//  transport->Timer(0, [](){f(new bool(true));})

//Callback into the main process eventBase (i.e. re-queue event and return control to process)
void ThreadPool::issueCallback(std::function<void(void *)> cb, void *arg, event_base *libeventBase) {
  EventInfo *info = GetUnusedEventInfo(); // new EventInfo(this);
  info->cb = std::move(cb);
  info->r = arg;
  // info->ev = event_new(libeventBase, -1, 0, ThreadPool::EventCallback, info);
  info->ev = GetUnusedEvent(libeventBase, info);
  event_add(info->ev, NULL);
  event_active(info->ev, 0, 0);
}

//Callback into the main (serial) thread
void ThreadPool::issueMainThreadCallback(std::function<void(void *)> cb, void *arg) {

  auto f = [cb, arg]() {
    cb(arg);
    return (void *)true;
  };
  main_thread_request_list.enqueue(std::move(f));
}

////////////////////////////////////////

ThreadPool::EventInfo *ThreadPool::GetUnusedEventInfo() {
  std::unique_lock<std::mutex> lock(EventInfoMutex);
  EventInfo *info;
  if (eventInfos.size() > 0) {
    info = eventInfos.back();
    eventInfos.pop_back();
  } else {
    info = new EventInfo(this);
  }
  return info;
}

void ThreadPool::FreeEventInfo(EventInfo *info) {
  std::unique_lock<std::mutex> lock(EventInfoMutex);
  eventInfos.push_back(info);
}

event *ThreadPool::GetUnusedEvent(event_base *libeventBase, EventInfo *info) {
  std::unique_lock<std::mutex> lock(EventMutex);
  event *event;
  if (events.size() > 0) {
    event = events.back();
    events.pop_back();
    event_assign(event, libeventBase, -1, 0, ThreadPool::EventCallback, info);
  } else {
    event = event_new(libeventBase, -1, 0, ThreadPool::EventCallback, info);
  }
  return event;
}

void ThreadPool::FreeEvent(event *event) {
  std::unique_lock<std::mutex> lock(EventMutex);
  event_del(event);
  events.push_back(event);
}

//INDEXED THREADPOOL

//Dispatch a job f to a worker thread of choice (with id), when done issue callback on main process (i.e. return control to eventBase)
void ThreadPool::dispatch_indexed(uint64_t id, std::function<void *()> f, std::function<void(void *)> cb, event_base *libeventBase) { 

  EventInfo *info = GetUnusedEventInfo();
  info->cb = std::move(cb);
  info->ev = GetUnusedEvent(libeventBase, info);
  event_add(info->ev, NULL);

  auto worker_id = id % total_indexed_workers;

  indexed_worker_thread_request_list[worker_id].enqueue(std::make_pair(std::move(f), info));
}
//Dispatch a job f to a worker thread of choice (with id), without any callback.
void ThreadPool::detach_indexed(uint64_t id, std::function<void *()> f) {
  EventInfo *info = nullptr;

  auto worker_id = id % total_indexed_workers;

  indexed_worker_thread_request_list[worker_id].enqueue(std::make_pair(std::move(f), info));
}


void ThreadPool::add_n_indexed(int num_threads) {
  
  cpu_set_t cpuset;
  sched_getaffinity(0, sizeof(cpuset), &cpuset);
  fprintf(stderr, "cpu_count  %d \n", CPU_COUNT(&cpuset));
  fprintf(stderr, "get_nprocs  %d \n", get_nprocs());

  fprintf(stderr, "Add %d new threads to indexed threadpool\n", num_threads);
  int num_cpus = std::thread::hardware_concurrency(); ///(2-hyperthreading);
  fprintf(stderr, "Num_cpus: %d \n", num_cpus);
  if (num_cpus > 8) {
    num_cpus = 8;
    fprintf(stderr, "Total Num_cpus on client downregulated to: %d \n", num_cpus);
  }
 
  running = true;
  for (uint32_t i = 0; i < num_threads; i++) {
    uint64_t worker_id = total_indexed_workers + i;

    // IndexWorkerMap::accessor w;
    // indexed_worker_thread_request_list.insert(w, worker_id);
    // w.release();
    indexed_worker_thread_request_list[worker_id];

    std::thread *t;
    t = new std::thread([this, worker_id] {
      while (true) {
        std::pair<std::function<void *()>, EventInfo *> job;

        indexed_worker_thread_request_list[worker_id].wait_dequeue(job);
        
        if (!running) {
          break;
        }

        if (job.second) {
          job.second->r = job.first();
          event_active(job.second->ev, 0, 0);
        } else {
          job.first();
        }
      }
    });

    uint32_t cpu_placement = worker_id % num_cpus;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      Panic("Error calling pthread_setaffinity_np: %d", rc);
    }
    Debug("MainThread running on CPU %d.", sched_getcpu());

    threads.push_back(t);
    t->detach();
  }
  
  total_indexed_workers += num_threads;
}